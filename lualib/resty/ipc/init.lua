local json = require("cjson.safe")
local client = require("resty.ipc.client")
local server = require("resty.ipc.server")
local forwarder = require("resty.ipc.forwarder")
local connection = require("resty.ipc.connection")
local queue = require("resty.ipc.queue")
local utils = require("resty.ipc.utils")
local message = require("resty.ipc.message")

local disable_listening = require("resty.ipc.disable_listening")
local is_timeout = utils.is_timeout
local is_closed = utils.is_closed
local get_worker_id = utils.get_worker_id
local get_worker_name = utils.get_worker_name
local is_forward_msg = message.is_forward
local is_req_recv_msg = message.is_req_recv
local is_resp_recv_msg = message.is_resp_recv


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
local _MT = { __index = _M }

function _M.read_thread(self, connection)
    local req_recv_queue = self._req_recv_queue
    local resp_recv_queue = self._resp_recv_queue
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

function _M.new(forwarders, opts)
    assert(type(forwarders) == "table", "expected a table, but got " .. type(forwarders))
    local self = {
        _forwarders = forwarders,
        _conns = {},
    }
    client.init(self)
    forwarder.init(self)
    return setmetatable(self, _MT)
end

function _M.init_worker(self)
    local listenings = self._listenings

    for wid, addr in pairs(listenings) do
        if wid ~= LOCAL_WID then
            local ok, err = disable_listening(addr)
            if not ok then
                return nil, "failed to disable listening addr " .. addr .. ": " .. err
            end
        end
    end
end

function _M.request(self, data, dst)
    return client.request(self, data, dst)
end

function _M.serve(self, callback)
    return server.serve(self, callback)
end

function _M.forwarder_hello(self)
    return forwarder.forwarder_hello(self)
end

return _M
