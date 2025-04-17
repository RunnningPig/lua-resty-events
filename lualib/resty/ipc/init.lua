local json = require("cjson.safe")
local client = require("resty.ipc.client")
local server = require("resty.ipc.server")
local forwarder = require("resty.ipc.forwarder")
local utils = require("resty.ipc.utils")
local message = require("resty.ipc.message")
local connection_selector = require("resty.ipc.connection_selector")

local disable_listening = require("resty.ipc.disable_listening")
local get_worker_id = utils.get_worker_id
local is_timeout = utils.is_timeout
local get_worker_name = utils.get_worker_name
local is_forward_msg = message.is_forward
local is_req_recv_msg = message.is_req_recv
local is_resp_recv_msg = message.is_resp_recv
local send_request = message.send_request
local send_response = message.send_response
local forward_message = message.forward
local select_connection = connection_selector.select_connection


local type = type
local assert = assert
local setmetatable = setmetatable
local pairs = pairs


local ngx = ngx -- luacheck: ignore
local log = ngx.log
local exiting = ngx.worker.exiting
local ERR = ngx.ERR


local json_encode = json.encode


local LOCAL_WID


local _M = {}
local _MT = { __index = _M }

function _M.read_thread(self, connection)
    local worker_id = connection.info.id
    while not exiting() do
        local msg, err = connection:recv_frame()
        if err then
            if not is_timeout(err) then
                return nil, err
            end

            -- timeout
            goto continue
        end

        if not msg then
            if not exiting() then --todo delete exiting?
                log(ERR, "did not receive msg from ", get_worker_name(worker_id))
            end
            goto continue
        end

        local is_req_recv_msg, req, src, req_id = is_req_recv_msg(msg)
        if is_req_recv_msg then
            server.received_request(self, req, src, req_id)
            goto continue
        end

        local is_resp_recv_msg, req_id, resp = is_resp_recv_msg(msg)
        if is_resp_recv_msg then
            client.received_response(self, resp, req_id)
            goto continue
        end

        local is_forward_msg, dst = is_forward_msg(msg)
        if is_forward_msg then
            forwarder.received_forward(self, msg, dst)
            goto continue
        end

        if not exiting() then
            log(ERR, "drop invalid msg. msg: ", json_encode(msg))
        end

        ::continue::
    end -- while not terminating

    return true
end

function _M.req_send_thread(self, connection)
    local req_send_queue = self._req_send_queues[connection.info.id]
    while not exiting() do
        local entity, err = req_send_queue:pop()
        if err then
            if not is_timeout(err) then
                log(ERR, "client#", LOCAL_WID, "req send semaphore wait error: " .. err)
                break
            end

            -- timeout
            goto continue
        end

        local req, dst, sem = entity[1], entity[2], entity[3]

        local connection, err = select_connection(self, dst)
        if not connection then
            if not exiting() and self._waiting_requests[sem] then
                self._sent_requests[sem] = { nil, "failed to select connection to send request: " .. err }
                sem:post()
            end
            goto continue
        end

        local req_id, err = send_request(connection, req, dst)
        if not req_id then
            if not exiting() and self._waiting_requests[sem] then
                self._sent_requests[sem] = { nil, err }
                sem:post()
            end
            goto continue
        end

        self._sent_requests[sem] = { req_id }
        sem:post()

        ::continue::
    end -- while not terminating

    return true
end

function _M.new(forwarders, opts)
    assert(type(forwarders) == "table", "expected a table, but got " .. type(forwarders))
    local self = {
        _forwarders = forwarders,
        _conns = {},
    }
    client.init(self, opts)
    forwarder.init(self, opts)
    return setmetatable(self, _MT)
end

function _M.init_worker(self)
    LOCAL_WID = get_worker_id()

    local _forwarders = self._forwarders
    for wid, addr in pairs(_forwarders) do
        if wid ~= LOCAL_WID then
            local ok, err = disable_listening(addr)
            if not ok then
                return nil, "failed to disable listening addr " .. addr .. ": " .. err
            end
        end
    end

    client.init_worker(self)
    forwarder.init_worker(self)
    server.init_worker(self)

    return true
end

function _M.request(self, data, dst)
    return client.request(self, data, dst)
end

function _M.serve(self, callback, opts)
    return server.serve(self, callback, opts)
end

function _M.forwarder_hello(self)
    return forwarder.forwarder_hello(self)
end

return _M
