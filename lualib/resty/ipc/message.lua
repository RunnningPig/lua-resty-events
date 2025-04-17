local select_connection = require("resty.ipc.connection_selector").select_connection
local get_worker_id = require("resty.ipc.utils").get_worker_id

local setmetatable = setmetatable


local REQ_MSG = 1
local RESP_MSG = 2

local REQ_SEQ = 1

local MSG_DATA = {}


local _M = {}


function _M.is_req_recv(msg)
    return msg.dst == get_worker_id() and msg.type == REQ_MSG, msg.data, msg.src, msg.seq
end

function _M.is_resp_recv(msg)
    return msg.dst == get_worker_id() and msg.type == RESP_MSG, msg.seq, msg.data
end

function _M.is_forward(msg)
    return msg.dst ~= get_worker_id(), msg.dst
end

function _M.send(conn, payload, dst)
    local msg = MSG_DATA
    msg.type = REQ_MSG
    msg.src = get_worker_id()
    msg.dst = dst
    msg.data = payload

    REQ_SEQ = REQ_SEQ + 1

    local _, err = conn:send_frame(msg)
    if err then
        return nil, "failed to send msg: " .. err
    end

    return true
end

function _M.send_request(conn, payload, dst)
    local msg = MSG_DATA
    msg.type = REQ_MSG
    msg.src = get_worker_id()
    msg.dst = dst
    msg.data = payload
    msg.seq = REQ_SEQ

    REQ_SEQ = REQ_SEQ + 1

    local _, err = conn:send_frame(msg)
    if err then
        return nil, "failed to send req msg: " .. err
    end

    return msg.seq
end

function _M.send_response(conn, payload, req_seq, dst)
    local msg = MSG_DATA
    msg.type = RESP_MSG
    msg.src = get_worker_id()
    msg.dst = dst
    msg.data = payload
    msg.seq = req_seq

    local _, err = conn:send_frame(msg)
    if err then
        return nil, "failed to send resp msg: " .. err
    end

    return msg.seq
end

function _M.forward(conn, msg)
    return conn:send_frame(msg)
end

return _M
