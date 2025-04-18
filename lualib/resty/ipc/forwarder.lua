local json = require("cjson.safe")
local connection = require("resty.ipc.connection")
local utils = require("resty.ipc.utils")
local message = require("resty.ipc.message")
local queue = require("resty.ipc.queue")

local get_worker_id = utils.get_worker_id
local is_closed = utils.is_closed
local forward_message = message.forward


local ngx = ngx -- luacheck: ignore
local log = ngx.log
local exit = ngx.exit
local exiting = ngx.worker.exiting
local ERR = ngx.ERR
local NOTICE = ngx.NOTICE


local spawn = ngx.thread.spawn
local kill = ngx.thread.kill
local wait = ngx.thread.wait


local json_encode = json.encode


local LOCAL_WID


local _M = {}


function _M.forwarder_hello(self)
    local _req_send_queues = self._req_send_queues
    if not _req_send_queues then
        log(ERR, "broker is not (yet) ready to accept connections on worker #", broker_id)
        return exit(444)
    end

    local client_connection, err = connection.forwarder_hello()
    if exiting() then
        if client_connection then
            client_connection:close()
        end
        return
    end

    if not client_connection then
        log(ERR, "forwarder#", LOCAL_WID, " failed to forwarder hello: ", err)
        return exit(ngx.ERR)
    end

    local client_id = client_connection.info.id
    local client_pid = client_connection.info.pid
    local client_is_forwader = self._forwarders[client_id] and true or false

    if client_is_forwader and client_id <= LOCAL_WID then
        log(ERR, "client#", client_id, "(pid: )", " connection rejected on forwarder#", LOCAL_WID,
            ": client id less than forwarder id")
        return exit(ngx.ERR)
    end

    local req_send_queue = self._req_send_queues[client_id]
    if not req_send_queue then
        req_send_queue = queue.new(self._max_req_send_queue_len)
        self._req_send_queues[client_id] = req_send_queue
    end

    self._conns[client_id] = client_connection

    local read_thread_co = spawn(self.read_thread, self, client_connection)
    local req_sent_thread_co = spawn(self.req_send_thread, self, client_connection, req_send_queue)

    log(NOTICE, "forwarder#", LOCAL_WID, " connected to client#", client_id, "(pid:", client_pid,
        ") and is ready to send/recv messages.")

    local ok, err, perr = wait(read_thread_co, req_sent_thread_co)

    self._conns[client_id] = nil

    if exiting() then
        kill(read_thread_co)
        kill(req_sent_thread_co)
        return
    end

    if not ok and not is_closed(err) then
        log(ERR, "forwarder#", LOCAL_WID, " disconnected with client#", client_id, "(pid: ", client_pid, "): ", err)
        return exit(ngx.ERROR)
    end

    if perr and not is_closed(err) then
        log(ERR, "forwarder#", LOCAL_WID, " disconnected with client#", client_id, "(pid: ", client_pid, "): ", perr)
        return exit(ngx.ERROR)
    end

    wait(read_thread_co)
    wait(req_sent_thread_co)

    return exit(ngx.OK)
end

function _M.init(self)
    return true
end

function _M.init_worker(self)
    LOCAL_WID = get_worker_id()
    return true
end

function _M.received_forward(self, msg, dst)
    local connection = self._conns[dst]
    if not connection then
        log(ERR, "forwarder#", LOCAL_WID, " failed to forward message to client#", dst,
            ": dest connection not found. msg: ", json_encode(msg))
        return
    end

    forward_message(connection, msg)
end

return _M
