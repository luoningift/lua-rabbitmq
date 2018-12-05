--
-- Copyright (C) 2016 Meng Zhang @ Yottaa,Inc
--

module("amqp", package.seeall)

local c = require "consts"
local frame = require "frame"
local logger = require "logger"
local tcp = ngx.socket.tcp
local rawget = rawget
local format = string.format
local gmatch = string.gmatch
local min = math.min

local amqp = {}
local mt = { __index = amqp }

--
-- initialize the context
--
function amqp.new(self, opts)

    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({
        sock = sock,
        opts = opts,
        connection_state = c.state.CLOSED,
        channel_state = c.state.CLOSED,
        major = c.PROTOCOL_VERSION_MAJOR,
        minor = c.PROTOCOL_VERSION_MINOR,
        revision = c.PROTOCOL_VERSION_REVISION,
        frame_max = c.DEFAULT_FRAME_SIZE,
        channel_max = c.DEFAULT_MAX_CHANNELS,
        mechanism = c.MECHANISM_PLAIN
    }, mt)
end

function amqp.set_timeout(self, timeout)
    local sock = rawget(self, "sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end

function amqp.set_keepalive(self, ...)
    local sock = rawget(self, "sock")
    if not sock then
        return nil, "not initialized"
    end
    return sock:setkeepalive(...)
end

function amqp.get_reused_times(self)
    local sock = rawget(self, "sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end

--
-- connect to the broker
--
function amqp.connect(self, ...)
    local sock = rawget(self, "sock")
    if not sock then
        return nil, "not initialized"
    end
    -- configurable but 5 seconds timeout
    sock:settimeout(self.opts.connect_timeout or 5000)

    local ok, err = sock:connect(...)
    if not ok then
        logger.error("[amqp.connect] failed: ", err)
        return nil, err
    end
    return true
end

--
-- to close the socket
--
function amqp.close(self)
    local sock = rawget(self, "sock")
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end

--
-- connection and channel
--
local function connection_start_ok(ctx)

    local user = ctx.opts.user or "guest"
    local password = ctx.opts.password or "guest"
    local f = frame.new_method_frame(c.DEFAULT_CHANNEL,
        c.class.CONNECTION,
        c.method.connection.START_OK)
    f.method = {
        properties = {
            product = c.PRODUCT,
            version = c.VERSION,
            platform = "posix",
            copyright = c.COPYRIGHT,
            capabilities = {
                authentication_failure_close = true
            }
        },
        mechanism = ctx.mechanism,
        response = format("\0%s\0%s", user, password),
        locale = c.LOCALE
    }

    return frame.wire_method_frame(ctx, f)
end

local function connection_tune_ok(ctx)

    local f = frame.new_method_frame(c.DEFAULT_CHANNEL,
        c.class.CONNECTION,
        c.method.connection.TUNE_OK)

    f.method = {
        channel_max = ctx.channel_max or c.DEFAULT_MAX_CHANNELS,
        frame_max = ctx.frame_max or c.DEFAULT_FRAME_SIZE,
        heartbeat = ctx.opts.heartbeat or c.DEFAULT_HEARTBEAT
    }

    local msg = f:encode()
    local sock = ctx.sock
    local bytes, err = sock:send(msg)
    if not bytes then
        return nil, "[connection_tune_ok]" .. err
    end
    logger.dbg("[connection_tune_ok] wired a frame.", "[class_id]: ", f.class_id, "[method_id]: ", f.method_id)
    return true
end

local function connection_open(ctx)
    local f = frame.new_method_frame(c.DEFAULT_CHANNEL,
        c.class.CONNECTION,
        c.method.connection.OPEN)
    f.method = {
        virtual_host = ctx.opts.virtual_host or "/"
    }

    return frame.wire_method_frame(ctx, f)
end

local function channel_open(ctx)

    local f = frame.new_method_frame(ctx.opts.channel or 1,
        c.class.CHANNEL,
        c.method.channel.OPEN)
    local msg = f:encode()
    local sock = ctx.sock
    local bytes, err = sock:send(msg)
    if not bytes then
        return nil, "[channel_open]" .. err
    end

    logger.dbg("[channel_open] wired a frame.", "[class_id]: ", f.class_id, "[method_id]: ", f.method_id)
    local res = frame.consume_frame(ctx)
    if res then
        logger.dbg("[channel_open] channel: ", res.channel)
    end
    return res
end

local function is_version_acceptable(ctx, major, minor)
    return ctx.major == major and ctx.minor == minor
end

local function is_mechanism_acceptable(ctx, method)
    local mechanism = method.mechanism
    if not mechanism then
        return nil, "broker does not support any mechanism."
    end

    for me in gmatch(mechanism, "%S+") do
        if me == ctx.mechanism then
            return true
        end
    end

    return nil, "mechanism does not match"
end

local function verify_capablities(ctx, method)

    if not is_version_acceptable(ctx, method.major, method.minor) then
        return nil, "protocol version does not match."
    end

    if not is_mechanism_acceptable(ctx, method) then
        return nil, "mechanism does not match."
    end
    return true
end


local function negotiate_connection_tune_params(ctx, method)
    if not method then
        return
    end

    if method.channel_max ~= nil and method.channel_max ~= 0 then
        -- 0 means no limit
        ctx.channel_max = min(ctx.channel_max, method.channel_max)
    end

    if method.frame_max ~= nil and method.frame_max ~= 0 then
        ctx.frame_max = min(ctx.frame_max, method.frame_max)
    end
end

local function set_state(ctx, channel_state, connection_state)
    ctx.channel_state = channel_state
    ctx.connection_state = connection_state
end

function amqp.setup(self)

    local count, err = self:get_reused_times()
    ngx.say(count)
    if 0 == count then
        local sock = rawget(self, "sock")
        if not sock then
            return nil, "not initialized"
        end

        local res, err = frame.wire_protocol_header(self)
        if not res then
            logger.error("[amqp.setup] wire_protocol_header failed: " .. err)
            return nil, err
        end

        if res.method then
            logger.dbg("[amqp.setup] connection_start: ", res.method)
            local ok, err = verify_capablities(self, res.method)
            if not ok then
                -- in order to close the socket without sending futher data
                set_state(self, c.state.CLOSED, c.state.CLOSED)
                return nil, err
            end
        end

        local res, err = connection_start_ok(self)
        if not res then
            logger.error("[amqp.setup] connection_start_ok failed: " .. err)
            return nil, err
        end

        negotiate_connection_tune_params(self, res.method)

        local res, err = connection_tune_ok(self)
        if not res then
            logger.error("[amqp.setup] connection_tune_ok failed: " .. err)
            return nil, err
        end

        local res, err = connection_open(self)
        if not res then
            logger.error("[amqp.setup] connection_open failed: " .. err)
            return nil, err
        end

        local res, err = channel_open(self)
        if not res then
            logger.error("[amqp.setup] channel_open failed: " .. err)
            return nil, err
        end
    elseif err then
        ngx.say("failed to get reused times: ", err)
        return nil, err
    end
    self.connection_state = c.state.ESTABLISHED
    self.channel_state = c.state.ESTABLISHED
    return true
end

function amqp.publish(self, payload)

    local size = #payload
    local f = frame.new_method_frame(self.channel or 1,
        c.class.BASIC,
        c.method.basic.PUBLISH)

    f.method = {
        exchange = self.opts.exchange,
        routing_key = self.opts.routing_key or "",
        mandatory = false,
        immediate = false
    }

    local msg = f:encode()
    local sock = rawget(self, "sock")
    local bytes, err = sock:send(msg)
    if not bytes then
        return nil, "[basic_publish]" .. err
    end
    local ok, err = frame.wire_header_frame(self, size)
    if not ok then
        logger.error("[amqp.publish] failed: " .. err)
        return nil, err
    end
    local ok, err = frame.wire_body_frame(self, payload)
    if not ok then
        logger.error("[amqp.publish] failed: " .. err)
        return nil, err
    end
    return true
end

return amqp

