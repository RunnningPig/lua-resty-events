local utils = require "resty.events.utils"
local events_broker = require "resty.events.broker"
local events_worker = require "resty.events.worker"
local disable_listening = require "resty.events.disable_listening"


local get_worker_id = utils.get_worker_id


local type = type
local setmetatable = setmetatable
local str_sub = string.sub


local worker_count = utils.get_worker_count()


local _M = {
    _VERSION = "0.3.1",
}
local _MT = { __index = _M, }


local function check_options(opts)
    assert(type(opts) == "table", "Expected a table, got " .. type(opts))

    local UNIX_PREFIX = "unix:"
    local DEFAULT_UNIQUE_TIMEOUT = 5
    local DEFAULT_MAX_QUEUE_LEN = 1024 * 10
    local DEFAULT_MAX_PAYLOAD_LEN = 1024 * 64       -- 64KB
    local LIMIT_MAX_PAYLOAD_LEN = 1024 * 1024 * 16  -- 16MB

    opts.broker_id = opts.broker_id or 0

    if type(opts.broker_id) ~= "number" then
        return nil, '"broker_id" option must be a number'
    end

    if opts.broker_id < 0 or opts.broker_id >= worker_count then
        return nil, '"broker_id" option is invalid'
    end

    if not opts.listening then
        return nil, '"listening" option required to start'
    end

    if type(opts.listening) ~= "string" then
        return nil, '"listening" option must be a string'
    end

    if str_sub(opts.listening, 1, #UNIX_PREFIX) ~= UNIX_PREFIX then
        return nil, '"listening" option must start with ' .. UNIX_PREFIX
    end

    opts.unique_timeout = opts.unique_timeout or DEFAULT_UNIQUE_TIMEOUT

    if type(opts.unique_timeout) ~= "number" then
        return nil, 'optional "unique_timeout" option must be a number'
    end

    if opts.unique_timeout <= 0 then
        return nil, '"unique_timeout" must be greater than 0'
    end

    opts.max_queue_len = opts.max_queue_len or DEFAULT_MAX_QUEUE_LEN

    if type(opts.max_queue_len) ~= "number" then
        return nil, '"max_queue_len" option must be a number'
    end

    if opts.max_queue_len <= 0 then
        return nil, '"max_queue_len" option is invalid'
    end

    opts.max_payload_len = opts.max_payload_len or DEFAULT_MAX_PAYLOAD_LEN

    if type(opts.max_payload_len) ~= "number" then
        return nil, '"max_payload_len" option must be a number'
    end

    if opts.max_payload_len <= 0 or opts.max_payload_len > LIMIT_MAX_PAYLOAD_LEN then
        return nil, '"max_payload_len" option is invalid'
    end

    if opts.enable_privileged_agent ~= nil and type(opts.enable_privileged_agent) ~= "boolean" then
        return nil, '"enable_privileged_agent" option must be a boolean'
    end

    opts.testing = opts.testing or false

    if type(opts.testing) ~= "boolean" then
        return nil, '"testing" option must be a boolean'
    end

    return true
end

function _M.new(opts)
    assert(check_options(opts))

    local self = {
        opts   = opts,
        broker = events_broker.new(opts),
        worker = events_worker.new(opts),
    }

    return setmetatable(self, _MT)
end

-- opts = {broker_id = n, listening = 'unix:...', unique_timeout = x,}
function _M:init_worker()
    local opts = self.opts

    local worker_id = get_worker_id()

    local is_broker = opts.broker_id == worker_id or
                      opts.testing   == true

    local ok, err

    if is_broker then
        -- enable listening on broker
        ok, err = self.broker:init()

    elseif worker_id >= 0 then
        -- disable listening on other workers
        ok, err = disable_listening(opts.listening)

    else
        -- privileged agent
        if opts.enable_privileged_agent == false then
            return true
        end

        ok = true
    end

    if not ok then
        return nil, err
    end

    ok, err = self.worker:init()
    if not ok then
        return nil, err
    end

    return true
end

function _M:run()
    return self.broker:run()
end

function _M:publish(target, source, event, data)
    return self.worker:publish(target, source, event, data)
end

function _M:subscribe(source, event, callback)
    return self.worker:subscribe(source, event, callback)
end

function _M:unsubscribe(id)
    return self.worker:unsubscribe(id)
end

function _M:is_ready()
    return self.worker:is_ready()
end

return _M
