local json = require("cjson.safe")
local client = require("resty.ipc.client")
local server = require("resty.ipc.server")
local connection = require("resty.ipc.connection")
local connection_selector = require("resty.ipc.connection_selector")
local queue = require("resty.ipc.queue")
local utils = require("resty.ipc.utils")
local message = require("resty.ipc.message")

local disable_listening = require("resty.ipc.disable_listening")
local select_connection = connection_selector.select_connection
local is_timeout = utils.is_timeout
local is_closed = utils.is_closed
local get_worker_id = utils.get_worker_id
local get_worker_name = utils.get_worker_name
local is_forward_msg = message.is_forward
local is_req_recv_msg = message.is_req_recv
local is_resp_recv_msg = message.is_resp_recv
local is_forward_message = message.is_forward
local forward_message = message.forward


local type = type
local assert = assert
local setmetatable = setmetatable
local pairs = pairs
local random = math.random


local ngx = ngx -- luacheck: ignore
local log = ngx.log
local exit = ngx.exit
local sleep = ngx.sleep
local exiting = ngx.worker.exiting
local ERR = ngx.ERR
local DEBUG = ngx.DEBUG
local NOTICE = ngx.NOTICE


local spawn = ngx.thread.spawn
local kill = ngx.thread.kill
local wait = ngx.thread.wait


local timer_at = ngx.timer.at


local json_encode = json.encode


local LOCAL_WID          = utils.get_worker_id()
local EVENTS_COUNT_LIMIT = 100
local EVENTS_POP_LIMIT   = 2000
local EVENTS_SLEEP_TIME  = 0.05

local REQ_MGS            = 1
local RESP_MGS           = 2

local EMPTY_T            = {}

local EVENT_T            = {
    source = '',
    event = '',
    data = '',
    wid = '',
}

local SPEC_T             = {
    unique = '',
}

local PAYLOAD_T          = {
    spec = EMPTY_T,
    data = '',
}


local _M = {}


-- gen a random number [0.01, 0.05]
-- it means that delay will be 10ms~50ms
local function random_delay()
    return random(10, 50) / 1000
end


local function process_req_recv_messages(premature, self)
    if premature then
        return true
    end

    self:process_events()

    return true
end

local function process_resp_recv_messages(premature, self)
    if premature then
        return true
    end

    self:process_events()

    return true
end


local function start_communicate_thread(self, delay)
    if exiting() then
        return
    end
    assert(timer_at(delay, communicate, self))
end
local function start_resp_recv_queue_process_thread(self)
    if exiting() then
        return
    end
    assert(timer_at(0, process_resp_recv_queue, self))
end

local function start_threads(self)
    start_communicate_thread(self, 0)

    if self.max_req_recv_queue_process_threads > 0 then
        for i = 1, self.max_req_recv_queueprocess_threads do
            start_req_recv_process_thread(self)
        end
    else
        start_req_recv_queue_process_threads(self)
    end

    if self.max_resp_recv_queue_process_threads > 0 then
        for i = 1, self.max_resp_recv_queue_process_threads do
            start_resp_recv_queue_process_thread(self)
        end
    else
        start_resp_recv_queue_process_threads(self)
    end
end


function _M.forwarder_hello(self)
    local connection, err = connection.forwarder_hello()
    if not connection then
        log(ERR, "failed to process server hello: ", err)
        return exit(ngx.ERR)
    end

    local worker_id = connection.info.id
    local worker_pid = connection.info.pid

    if worker_id <= LOCAL_WID then
        log(ERR, "client#", worker_id, " connection rejected on forwarder#", LOCAL_WID,
            ": client id less than forwarder id")
        return exit(ngx.ERR)
    end

    local connections = self._conns
    if connections[worker_id] then
        log(ERR, "client#", worker_id, " connection rejected on forwarder#", LOCAL_WID, ": connection exist")
        return exit(ngx.ERR)
    end

    connections[worker_id] = connection

    local read_thread_co = spawn(self.read_thread, self, connection)

    log(NOTICE, get_worker_name(worker_id),
        " connected to forwarder#", LOCAL_WID, "(worker pid: ", worker_id, ")")

    local ok, err, perr = wait(read_thread_co)

    connections[worker_id] = nil

    if exiting() then
        kill(read_thread_co)
        return
    end

    if not ok and not is_closed(err) then
        log(ERR, "forwarder#", LOCAL_WID, " failed on ", get_worker_name(worker_id),
            ": ", err, " (worker pid: ", worker_pid, ")")
        return exit(ngx.ERROR)
    end

    if perr and not is_closed(perr) then
        log(ERR, "forwarder#", LOCAL_WID, " failed on ", get_worker_name(worker_id),
            ": ", perr, " (worker pid: ", worker_pid, ")")
        return exit(ngx.ERROR)
    end

    wait(read_thread_co)

    return exit(ngx.OK)
end

function _M.init(self, opts)
    return true
end

function _M.init_worker()
    return true
end

function _M.received_forward(self, msg, dst)
    local connection = self._conns[dst]
    if not connection then
        log(ERR, "forwarder#", LOCAL_WID, " failed to forward message to ", get_worker_name(dst),
            ": dest connection not found", ". msg: ", json_encode(msg))
        return
    end

    forward_message(connection, msg)
end

return _M
