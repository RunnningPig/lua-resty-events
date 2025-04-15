local ipc = require("resty.ipc")

local opts = {
    forwarders = {
        "unix:/tmp/resty.ipc.0.sock",
        "unix:/tmp/resty.ipc.1.sock",
    }
}
ipc = ipc.new(opts)

local client, server, forwarder = ipc:init_worker()


server.serve(function(data, src_wid)
    local resp_data = process(data)
    server.replyto(resp_data, src_wid)
end)

forwarder:forward()

local res, err = client:requestto(data, dst_wid)
local ok, err = client:sendto(data, dst_wid)



-- 1. 支持多个 forwarder;
-- 2. 支持 req/resp 模式;
-- 3. one by one request
-- 4. batch request
-- 5.

--[[*
ipc-send:
    packet = send_queue.pop()
    conn = connection[packet.dst]
    conn.send(package.paylaod)



ipc-recv:
    packet = conn.recv()
    package = recv_queue.pop()


frame-send:
    packet = send_queue.pop()
    packet
    conn.send()

req-send-thread:
    while not exiting:
        req = req_queue.pop()
        conn = connections[req.dst]
        if not conn:
            conn = forwarders[random]

        req = {
            data = req.data,
            dst = req.dst,

            seq = self.seq,
            src = wid,
            type = REQ,
        }
        self.seq++
        conn.send_req(req)
        wait_resp_queue[req.seq] = req.sem

resp-recv-thread for conns:
    while not exiting:
        resp = conn.recv_resp()


forward-thread:


ipc
    request(data, dst_wid)
        conn = connections[dst_wid]
        if not conn then
            conn = connections[random]  -- send to forwarder
        end

        req = {
            type = REQ,
            seq = self.seq,
            src = local_wid,
            dst = dst_wid,
            data = data,
        }
        conn.send_frame(req)

        wait_resp_queue[req.seq] = semaphore.new()

        self.seq++


    client_hello()

    server_hello()
        info = protocol.recv_frame()

        if is_server(wid) and is_server(info.wid) and wid > info.wid then
            log(ERROR, "server #", wid, got connection from another server #", info.id)
            return exit(444)
        end

        connections[info.id] = connection

        spawn(read_thread, connection)
        spawn(write_thread, connection)

    serve(on_req)




connection
    sock

    send_frame(frame):
        payload = encode(frame)
        payload_len = #payload
        sock.send(int32(payload_len))
        sock.send(payload)

    recv_frame():
        payload_len = sock.recv(int32)
        payload = sock.recv(payload_len)
        frame = decode(payload)
        return frame

    close():
        sock.close()


protocol
    send_frame(frame)
        payload = encode(frame)
        payload_len = #payload
        sock.send(int32(payload_len))
        sock.send(payload)
        return ok

    recv_frame()
        payload_len = sock.recv(int32)
        payload = sock.recv(payload_len)
        frame = decode(payload)
        return frame
]]
