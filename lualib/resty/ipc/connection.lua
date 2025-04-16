local codec = require("resty.ipc.codec")
local frame = require("resty.ipc.frame")
local utils = require("resty.ipc.utils")


local encode = codec.encode
local decode = codec.decode
local send_frame = frame.send
local recv_frame = frame.recv
local get_worker_id = utils.get_worker_id

local ngx = ngx
local worker_pid = ngx.worker.pid
local tcp = ngx.socket.tcp
local req_sock = ngx.req.socket
local ngx_header = ngx.header
local ngx_send_headers = ngx.send_headers
local ngx_flush = ngx.flush
local subsystem = ngx.config.subsystem -- todo: what


local type = type
local str_sub = string.sub
local str_find = string.find
local setmetatable = setmetatable


-- for high traffic pressure
local DEFAULT_TIMEOUT = 5000 -- 5000ms


local _M = {}
local _MT = { __index = _M }

function _M.client_hello(addr)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end

    sock:settimeout(DEFAULT_TIMEOUT)

    if type(addr) ~= "string" then
        return nil, "addr must be a string"
    end

    if str_sub(addr, 1, 5) ~= "unix:" then
        return nil, "addr must start with \"unix:\""
    end

    local ok, err = sock:connect(addr)
    if not ok then
        return nil, "failed to connect: " .. err
    end

    if subsystem == "http" then
        local req = "GET / HTTP/1.1\r\n" ..
            "Host: localhost\r\n" ..
            "Connection: Upgrade\r\n" ..
            "Upgrade: Resty-IPC/1\r\n\r\n"

        local bytes, err = sock:send(req)
        if not bytes then
            return nil, "failed to send the handshake request: " .. err
        end

        local header_reader = sock:receiveuntil("\r\n\r\n")
        local header, err, _ = header_reader()
        if not header then
            return nil, "failed to receive response header: " .. err
        end

        if str_find(header, "HTTP/1.1 ", nil, true) ~= 1 then
            return nil, "bad HTTP response status line: " .. header
        end
    end -- subsystem == "http"

    local info = {
        id = get_worker_id(),
        pid = worker_pid(),
    }

    local _, err = send_frame(sock, encode(info))
    if err then
        return nil, "failed to send client hello info: " .. err
    end

    local data, err = recv_frame(sock)
    if not data then
        return nil, "failed to recv server hello info: " .. err
    end

    local info, err = decode(data)
    if err then
        return nil, "invalid server hello info received: " .. err
    end

    local self = {
        info = info,
        sock = sock,
    }

    return setmetatable(self, _MT)
end

function _M.forwarder_hello()
    if subsystem == "http" then
        if ngx.headers_sent then
            return nil, "response header already sent"
        end

        ngx_header["Upgrade"] = "Resty-IPC/1"
        ngx_header["Content-Type"] = nil
        ngx.status = 101

        local ok, err = ngx_send_headers()
        if not ok then
            return nil, "failed to send response header: " .. (err or "unknown")
        end

        ok, err = ngx_flush(true)
        if not ok then
            return nil, "failed to flush response header: " .. (err or "unknown")
        end
    end -- subsystem == "http"

    local sock, err = req_sock(true)
    if not sock then
        return nil, err
    end

    sock:settimeout(DEFAULT_TIMEOUT)

    local data, err = recv_frame(sock)
    if err then
        return nil, "failed to read worker info: " .. err
    end

    local info, err = decode(data)
    if err then
        return nil, "invalid client hello info received: " .. err
    end

    local info = {
        id = get_worker_id(),
        pid = worker_pid(),
    }

    local _, err = send_frame(sock, encode(info))
    if err then
        return nil, "failed to server hello info: " .. err
    end

    local self = {
        info = info,
        sock = sock,
    }

    return setmetatable(self, _MT)
end

function _M.send_frame(self, payload)
    return send_frame(self.sock, payload)
end

function _M.recv_frame(self)
    return recv_frame(self.sock)
end

function _M.close(self)
    return self.sock:close()
end

return _M
