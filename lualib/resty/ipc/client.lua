local select_connection = require("ipc.connection_selector").select_connection
local message = require("resty.ipc.message")

local cjson = require "cjson.safe"
local codec = require "resty.events.codec"
local queue = require "resty.events.queue"
local callback = require "resty.events.callback"
local utils = require "resty.events.utils"


local frame_validate = require("resty.events.frame").validate
local client = require("resty.events.protocol").client
local is_timeout = utils.is_timeout
local get_worker_id = utils.get_worker_id
local get_worker_name = utils.get_worker_name
local is_resp_recv_msg = message.is_resp_recv_msg
local send_request = message.send_request


local type = type
local assert = assert
local setmetatable = setmetatable
local random = math.random


local ngx = ngx -- luacheck: ignore
local log = ngx.log
local sleep = ngx.sleep
local exiting = ngx.worker.exiting
local ERR = ngx.ERR
local DEBUG = ngx.DEBUG
local NOTICE = ngx.NOTICE


local semaphore = ngx.semaphore
local spawn = ngx.thread.spawn
local kill = ngx.thread.kill
local wait = ngx.thread.wait


local timer_at = ngx.timer.at


local encode = codec.encode
local decode = codec.decode
local cjson_encode = cjson.encode


local LOCAL_WID                 = get_worker_id()

local DEFAULT_REQ_TIMEOUT       = 3
local RESP_RECV_QUEUE_POP_LIMIT = 2000


local EMPTY_T = {}

local EVENT_T = {
    source = '',
    event = '',
    data = '',
    wid = '',
}

local SPEC_T = {
    unique = '',
}

local PAYLOAD_T = {
    spec = EMPTY_T,
    data = '',
}


local _M = {}

-- gen a random number [0.01, 0.05]
-- it means that delay will be 10ms~50ms
local function random_delay()
    return random(10, 50) / 1000
end

local check_sock_exist
do
    local ffi = require "ffi"
    local C = ffi.C
    ffi.cdef [[
        int access(const char *pathname, int mode);
    ]]

    -- remove prefix 'unix:'
    check_sock_exist = function(fpath)
        local rc = C.access(fpath:sub(6), 0)
        return rc == 0
    end
end

local start_communicate_timer
local function communicate(premature, self, addr)
    if premature then
        return true
    end

    local listening = self._opts.listening

    if not check_sock_exist(listening) then
        log(DEBUG, "unix domain socket (", listening, ") is not ready")

        -- try to reconnect broker, avoid crit error log
        start_communicate_timer(self, 0.002)
        return
    end

    local broker_connection = assert(client.new())

    local ok, err = broker_connection:connect(listening)

    if exiting() then
        if ok then
            broker_connection:close()
        end
        return
    end

    if not ok then
        log(ERR, "failed to connect: ", err)

        -- try to reconnect broker
        start_communicate_timer(self, random_delay())

        return
    end

    self._connected = true

    local read_thread_co = spawn(read_thread, self, broker_connection)
    local write_thread_co = spawn(write_thread, self, broker_connection)

    log(NOTICE, get_worker_name(self._worker_id),
        " is ready to accept events from ", listening)

    local ok, err, perr = wait(read_thread_co, write_thread_co)

    self._connected = nil

    if exiting() then
        kill(read_thread_co)
        kill(write_thread_co)

        broker_connection:close()

        return
    end

    if not ok then
        log(ERR, "event worker failed to communicate with broker (", err, ")")
    end

    if perr then
        log(ERR, "event worker failed to communicate with broker (", perr, ")")
    end

    wait(read_thread_co)
    wait(write_thread_co)

    broker_connection:close()

    start_communicate_timer(self, addr, random_delay())

    return true
end

local function start_communicate_timer(self, addr, delay)
    if exiting() then
        return
    end
    assert(timer_at(delay, communicate, self, addr))
end


local function start_communicate_timers(self)
    local forwarders = self._forwarders
    local is_forwader = forwarders[LOCAL_WID] and true or false
    for wid, addr in ipairs(forwarders) do
        if not is_forwader or wid < LOCAL_WID then
            start_communicate_timer(self, addr, 0)
        end
    end
end
local function start_timers(self)
    start_communicate_timers(self)
end


function _M.init(self, opts)
    return true
end

function _M.init_worker(self)
    start_timers(self)
    return true
end

function _M.request(self, payload, dst_wid, opts)
    local timout = opts.timeout or DEFAULT_REQ_TIMEOUT

    local connection, err = select_connection(self, dst_wid)
    if not connection then
        return nil, "failed to select connection: " .. err
    end

    local req_id, err = send_request(connection, payload, dst_wid)
    if not req_id then
        return nil, err
    end

    local sem = semaphore.new()
    self._waiting_responses[req_id] = sem

    local ok, err = sem:wait(timout)
    self._waiting_responses[req_id] = nil
    if not ok then
        return nil, err
    end

    local resp = self._received_responses[req_id]
    if not resp then
        return nil, "empty resp"
    end

    return resp
end

function _M.received_response(self, resp, req_id)
    local sem = self._waiting_responses[req_id]
    if sem then
        self._received_responses[req_id] = resp
        sem:post()
    end
end

return _M
