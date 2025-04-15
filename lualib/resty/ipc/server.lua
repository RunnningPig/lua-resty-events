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
local send_response = message.send_response


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

local _M                 = {}


local function process_request(self, req, src, req_id)
    local callback = self._client_callback
    if not callback then
        return nil, "server no start"
    end

    local resp, err = callback(req)
    if err then
        return nil, err
    end

    if req_id and not resp then
        return nil, "need reply but got empty resp"
    end

    local connection, err = select_connection(self, src)
    if not connection then
        return nil, "failed to select connection to send resp: " .. err
    end

    local _, err = send_response(connection, resp, req_id)
    if err then
        return nil, "failed to send resp: " .. err
    end

    return true
end


local function req_recv_queue_async_thread(self)
    local req_recv_queue = self._req_recv_queue
    while not exiting() do
        local msg, err = req_recv_queue:pop()
        if err then
            if not is_timeout(err) then
                log(ERR, "semaphore wait error: " .. err)
                break
            end

            -- timeout
            goto continue
        end

        local is_req_recv_msg, req, src, seq = is_req_recv_msg(msg)
        if not is_req_recv_msg then
            log(ERR, "invalid req recv msg:  ", json_encode(msg))
            goto continue
        end

        spawn(process_request, self, req, src, seq)

        ::continue::
    end
end

local start_req_recv_queue_process_async_thread
local function process_req_recv_queue_async(premature, self)
    if premature then
        return true
    end

    local req_recv_queue_thread_co = spawn(req_recv_queue_async_thread, self)

    log(NOTICE, get_worker_name(LOCAL_WID), " spawn thread to process req recv queue")

    local ok, err = wait(req_recv_queue_thread_co)

    if exiting() then
        kill(req_recv_queue_thread_co)
        return
    end

    if not ok then
        log(ERR, "req recv queue thread killed on ", get_worker_name(LOCAL_WID), ": ", err)
    else
        log(NOTICE, "req recv queue thread exit normally on ", get_worker_name(LOCAL_WID))
    end

    start_req_recv_queue_process_async_thread(self)
end


local function start_req_recv_queue_process_async_thread(self)
    if exiting() then
        return
    end
    assert(timer_at(0, process_req_recv_queue_async, self))
end

local function req_recv_queue_thread(self)
    local req_recv_queue = self._req_recv_queue
    while not exiting() do
        local entity, err = req_recv_queue:pop()
        if err then
            if not is_timeout(err) then
                log(ERR, "semaphore wait error: " .. err)
                break
            end

            -- timeout
            goto continue
        end

        local req, src, seq = entity[1], entity[2], entity[3]
        local ok, err = process_request(self, req, src, seq)
        if not ok then
            log(ERR, "failed to process request:  ", err)
            goto continue
        end

        ::continue::
    end
end

local start_req_recv_queue_process_thread
local function process_req_recv_queue(premature, self)
    if premature then
        return true
    end

    local req_recv_queue_thread_co = spawn(req_recv_queue_thread, self)

    log(NOTICE, get_worker_name(LOCAL_WID), " spawn thread to process req recv queue")

    local ok, err, perr = wait(req_recv_queue_thread_co)

    if exiting() then
        kill(req_recv_queue_thread_co)
        return
    end

    if not ok then
        log(ERR, "req recv queue thread killed on ", get_worker_name(LOCAL_WID), ": ", err)
    else
        log(NOTICE, "req recv queue thread exit normally on ", get_worker_name(LOCAL_WID))
    end

    if perr then
        log(ERR, "req recv queue thread killed on ", get_worker_name(LOCAL_WID), ": ", perr)
    end

    start_req_recv_queue_process_thread(self)
end


local function start_req_recv_queue_process_thread(self)
    if exiting() then
        return
    end
    assert(timer_at(0, process_req_recv_queue, self))
end

local function start_threads(self)
    if self.max_req_recv_queue_threads > 0 then
        for i = 1, self.max_req_recv_queue_threads do
            start_req_recv_queue_process_thread(self)
        end
    else
        start_req_recv_queue_process_async_thread(self)
    end
end

function _M.init(self, opts)
    local max_queue_len = opts.max_req_recv_queue_len or 256
    local max_req_recv_queue_threads = opts.max_rqueue_threads or 0
    self._resp_recv_queue = queue.new(max_queue_len)
    self._max_req_recv_queue_threads = max_req_recv_queue_threads
    return true
end

function _M.init_worker(self)
    start_threads(self)
    return true
end

function _M.serve(self, callback)
    self._client_callback = callback
end

function _M.received_request(self, req, src, req_id)
    local ok, err = self._req_recv_queue:push({ req, src, req_id }) --todo table.pool
    if not ok then
        log(ERR, "server# ", LOCAL_WID, " failed to store request#", req_id " from ", get_worker_name(src), ": ", err,
            ". req: ", json_encode(req))
    end
end

return _M
