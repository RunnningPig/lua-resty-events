local message = require("resty.ipc.message")
local connection = require("resty.ipc.connection")
local connection_selector = require("resty.ipc.connection_selector")
local utils = require("resty.ipc.utils")


local get_worker_id = utils.get_worker_id
local send_request = message.send_request
local select_connection = connection_selector.select_connection


local ngx = ngx -- luacheck: ignore
local log = ngx.log
local exiting = ngx.worker.exiting
local ERR = ngx.ERR
local DEBUG = ngx.DEBUG
local NOTICE = ngx.NOTICE


local semaphore = ngx.semaphore
local spawn = ngx.thread.spawn
local kill = ngx.thread.kill
local wait = ngx.thread.wait


local timer_at = ngx.timer.at


local assert = assert
local pairs = pairs
local random = math.random


local LOCAL_WID
local DEFAULT_REQ_TIMEOUT = 3


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


local start_client_hello_timer
local function client_hello(premature, self, forwarder_id)
    if premature then
        return true
    end

    local addr = self._forwarders[forwarder_id]
    if not addr then
        log(ERR, "client#", LOCAL_WID, " failed to client hello to non-forwarder#", forwarder_id)
        return
    end

    if not check_sock_exist(addr) then
        log(DEBUG, "unix domain socket (", addr, ") is not ready")

        -- try to reconnect broker, avoid crit error log
        start_client_hello_timer(self, forwarder_id, 0.002)
        return
    end

    local forwarder_connection, err = connection.client_hello(addr)
    if exiting() then
        if forwarder_connection then
            forwarder_connection:close()
        end
        return
    end

    if not forwarder_connection then
        log(ERR, "client#", LOCAL_WID, " failed to client hello to forwarder#", forwarder_id, ": ", err, ". addr: ", addr)

        -- try to reconnect forwarder
        start_client_hello_timer(self, forwarder_id, random_delay())

        return
    end

    local forwarder_id = forwarder_connection.info.id
    local forwarder_pid = forwarder_connection.info.pid

    self._conns[forwarder_id] = forwarder_connection

    local read_thread_co = spawn(self.read_thread, self, forwarder_connection)

    log(NOTICE, "client#", LOCAL_WID, " connected to forwarder#", forwarder_id, "(pid: ", forwarder_pid,
        ") and is ready to send/recv messages.")

    local ok, err = wait(read_thread_co)

    self._conns[forwarder_id] = nil

    if exiting() then
        kill(read_thread_co)

        forwarder_connection:close()

        return
    end

    if not ok then
        log(ERR, "client#", LOCAL_WID, " disconnected with forwarder#", forwarder_id, "(pid: ", forwarder_id, "): ", err)
    end

    forwarder_connection:close()

    start_client_hello_timer(self, forwarder_id, random_delay())

    return true
end


function start_client_hello_timer(self, wid, delay)
    if exiting() then
        return
    end
    assert(timer_at(delay, client_hello, self, wid))
end

local function start_client_hello_timers(self)
    local forwarders = self._forwarders
    local is_forwader = forwarders[LOCAL_WID] and true or false
    for wid in pairs(forwarders) do
        if (not is_forwader and wid ~= LOCAL_WID) or wid < LOCAL_WID then
            start_client_hello_timer(self, wid, 0)
        end
    end
end


local function start_timers(self)
    start_client_hello_timers(self)
end


function _M.init(self)
    self._waiting_responses = {}
    self._received_responses = {}
    return true
end

function _M.init_worker(self)
    LOCAL_WID = get_worker_id()
    start_timers(self)
    return true
end

function _M.request(self, payload, dst_wid, opts)
    local opts = opts or {}
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
