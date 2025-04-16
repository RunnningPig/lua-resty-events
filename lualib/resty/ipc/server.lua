local json = require("cjson.safe")
local connection_selector = require("resty.ipc.connection_selector")
local queue = require("resty.ipc.queue")
local utils = require("resty.ipc.utils")
local message = require("resty.ipc.message")


local get_worker_id = utils.get_worker_id
local select_connection = connection_selector.select_connection
local is_timeout = utils.is_timeout
local send_response = message.send_response


local assert = assert


local ngx = ngx -- luacheck: ignore
local log = ngx.log
local exiting = ngx.worker.exiting
local ERR = ngx.ERR
local NOTICE = ngx.NOTICE


local spawn = ngx.thread.spawn
local kill = ngx.thread.kill
local wait = ngx.thread.wait


local timer_at = ngx.timer.at


local LOCAL_WID


local _M = {}


local function process_request(self, req, src, req_id)
    local process_request = self._process_request
    local resp, err = process_request(req)
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
        local entity, err = req_recv_queue:pop()
        if err then
            if not is_timeout(err) then
                log(ERR, "server#", LOCAL_WID, " semaphore wait error: " .. err)
                break
            end

            -- timeout
            goto continue
        end

        local req, src, req_id = entity[1], entity[2], entity[3]

        spawn(process_request, self, req, src, req_id)

        ::continue::
    end
end

local start_req_recv_queue_process_async_thread
local function process_req_recv_queue_async(premature, self)
    if premature then
        return true
    end

    local req_recv_queue_thread_co = spawn(req_recv_queue_async_thread, self)

    log(NOTICE, "server#", LOCAL_WID, " spawn thread to process req recv queue")

    local ok, err = wait(req_recv_queue_thread_co)

    if exiting() then
        kill(req_recv_queue_thread_co)
        return
    end

    if not ok then
        log(ERR, "req recv queue thread killed on server#", LOCAL_WID, ": ", err)
    else
        log(NOTICE, "req recv queue thread exit normally on server#", LOCAL_WID)
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
                log(ERR, "server#", LOCAL_WID, " semaphore wait error: " .. err)
                break
            end

            -- timeout
            goto continue
        end

        local req, src, req_id = entity[1], entity[2], entity[3]
        local ok, err = process_request(self, req, src, req_id)
        if not ok then
            log(ERR, "server#", LOCAL_WID, "failed to process request#", req_id, " from client#", src, ": ", err)
            goto continue
        end

        ::continue::
    end
end

--todo 代码重复简化？
local start_req_recv_queue_process_thread
local function process_req_recv_queue(premature, self)
    if premature then
        return true
    end

    local req_recv_queue_thread_co = spawn(req_recv_queue_thread, self)

    log(NOTICE, "server#", LOCAL_WID, " spawn thread to process req recv queue")

    local ok, err = wait(req_recv_queue_thread_co)

    if exiting() then
        kill(req_recv_queue_thread_co)
        return
    end

    if not ok then
        log(ERR, "req recv queue thread killed on server#", LOCAL_WID, ": ", err)
    else
        log(NOTICE, "req recv queue thread exit normally on server#", LOCAL_WID)
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
    if self._max_req_recv_queue_threads > 0 then
        for i = 1, self._max_req_recv_queue_threads do
            start_req_recv_queue_process_thread(self)
        end
    else
        start_req_recv_queue_process_async_thread(self)
    end
end

function _M.init_worker(server)
    LOCAL_WID = get_worker_id()
    return true
end

function _M.serve(self, callback, opts)
    local opts = opts or {}
    if self._process_request then
        return nil, "server has started"
    end
    self._process_request = callback
    self._req_recv_queue = queue.new(opts.max_req_recv_queue_len or 256)
    self._max_req_recv_queue_threads = opts.max_req_recv_queue_threads or 0
    start_threads(self)
    return true
end

function _M.received_request(self, req, src, req_id)
    if not self._process_request then
        log(ERR, "server#", LOCAL_WID, " no start")
        return nil
    end

    local ok, err = self._req_recv_queue:push({ req, src, req_id }) --todo table.pool
    if not ok then
        log(ERR, "server# ", LOCAL_WID, " failed to store request#", req_id " from client#", src, ": ", err)
    end
end

return _M
