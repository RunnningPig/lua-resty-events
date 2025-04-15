local assert
local random = math.random


local _M = {}


function _M.select_connection(self, dst)
    local connections = self._conns
    local connection = connections[dst]
    if connection then
        return connection
    end

    local forwarders = {}
    for wid in ipairs(connections) do
        if wid > dst then
            break
        end
        forwarders[#forwarders + 1] = wid
    end

    local count = #forwarders
    if count == 0 then
        return nil, "dest is unreachable"
    end

    local forwarder = forwarders[count == 1 and 1 or random(count)]
    connection = connections[forwarder]

    return connection
end

return _M
