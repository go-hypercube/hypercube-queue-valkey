package valkeyqueue

import "github.com/valkey-io/valkey-go"

// Every key is explicitly declared and shares one Cluster hash tag. The final
// four arguments carry the prefix-wide policy; it is initialized atomically,
// versioned, and checked on every operation, including maintenance and reads.
const scriptPrelude = `
local messages = KEYS[1]
local attempts = KEYS[2]
local visibility = KEYS[3]
local delays = KEYS[4]
local tokens = KEYS[5]
local queues = KEYS[6]
local pending = KEYS[7]
local inflight = KEYS[8]
local dead = KEYS[9]
local deadExpiry = KEYS[10]
local retainedBytes = KEYS[11]
local policy = KEYS[12]

local p = #ARGV - 3
local retention = tonumber(ARGV[p])
local maxMessages = tonumber(ARGV[p + 1])
local maxMessageBytes = tonumber(ARGV[p + 2])
local maxBytes = tonumber(ARGV[p + 3])
local requested = '2:' .. table.concat({ARGV[p], ARGV[p+1], ARGV[p+2], ARGV[p+3]}, ':')
local configured = redis.call('GET', policy)
if configured then
    if configured ~= requested then
        return redis.error_reply('hypercube queue: configuration mismatch')
    end
else
    if redis.call('HLEN', messages) > 0 then
        return redis.error_reply('hypercube queue: incompatible storage; use a new prefix')
    end
    redis.call('SET', policy, requested)
end

local function now()
    local t = redis.call('TIME')
    return tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
end

local function used_bytes()
    return tonumber(redis.call('GET', retainedBytes) or '0')
end

local function adjust_bytes(delta)
    local remaining = redis.call('INCRBY', retainedBytes, delta)
    if remaining == 0 then
        redis.call('DEL', retainedBytes)
    end
end

local function current(id, token, time)
    if redis.call('HGET', tokens, id) ~= token then
        return false
    end
    local deadline = redis.call('ZSCORE', inflight, id)
    return deadline and tonumber(deadline) > time
end

local function remove(id, name, encoded)
    if redis.call('HEXISTS', messages, id) == 0 then
        error('hypercube queue: missing message record')
    end
    local size = redis.call('HSTRLEN', messages, id)
    redis.call('HDEL', messages, id)
    redis.call('HDEL', attempts, id)
    redis.call('HDEL', visibility, id)
    redis.call('HDEL', delays, id)
    redis.call('HDEL', tokens, id)
    redis.call('ZREM', pending, id)
    redis.call('ZREM', inflight, id)
    redis.call('ZREM', dead, id)
    redis.call('ZREM', deadExpiry, encoded .. ':' .. id)
    adjust_bytes(-size)
    local remaining = redis.call('HINCRBY', queues, name, -1)
    if remaining == 0 then
        redis.call('HDEL', queues, name)
    end
end

local function prune_queue(cutoff, name, encoded, limit)
    local ids = redis.call('ZRANGEBYSCORE', dead, '-inf', cutoff, 'LIMIT', 0, limit)
    for _, id in ipairs(ids) do
        remove(id, name, encoded)
    end
    return #ids
end
`

var pushScript = valkey.NewLuaScript(scriptPrelude + `
local id = ARGV[1]
if redis.call('HEXISTS', messages, id) == 1 then
    return 0
end
if ARGV[7] == '1' or #ARGV[2] > maxMessageBytes then
    return 3
end
if redis.call('HLEN', messages) >= maxMessages then
    return 2
end
if used_bytes() + #ARGV[2] > maxBytes then
    return 4
end
redis.call('HSET', messages, id, ARGV[2])
redis.call('HSET', attempts, id, 0)
redis.call('HSET', visibility, id, ARGV[3])
redis.call('HSET', delays, id, ARGV[4])
redis.call('ZADD', pending, now() + tonumber(ARGV[5]), id)
redis.call('HINCRBY', queues, ARGV[6], 1)
adjust_bytes(#ARGV[2])
return 1
`)

var popScript = valkey.NewLuaScript(scriptPrelude + `
local time = now()
local limit = tonumber(ARGV[1])
local ids = redis.call('ZRANGEBYSCORE', inflight, '-inf', time, 'LIMIT', 0, limit)
local ready = redis.call('ZRANGEBYSCORE', pending, '-inf', time, 'LIMIT', 0, limit - #ids)
for _, id in ipairs(ready) do
    table.insert(ids, id)
end
local result = {}
for _, id in ipairs(ids) do
    local data = redis.call('HGET', messages, id)
    local timeout = tonumber(redis.call('HGET', visibility, id))
    local attempt = tonumber(redis.call('HGET', attempts, id))
    if not data or not timeout or not attempt then
        return redis.error_reply('hypercube queue: missing message record')
    end
    table.insert(result, {data, attempt + 1, ARGV[3] .. ':' .. id})
end
for i, id in ipairs(ids) do
    local timeout = tonumber(redis.call('HGET', visibility, id))
    if timeout == 0 then
        timeout = tonumber(ARGV[2])
    end
    redis.call('ZREM', pending, id)
    redis.call('ZADD', inflight, time + timeout, id)
    redis.call('HINCRBY', attempts, id, 1)
    redis.call('HSET', delays, id, 0)
    redis.call('HSET', tokens, id, result[i][3])
end
return result
`)

// Ack and pruning only reduce retained state. They must still be available
// when noeviction rejects allocating scripts at the server's memory limit.
const freeingScriptFlags = "#!lua flags=allow-oom\n"

var ackScript = valkey.NewLuaScript(freeingScriptFlags + scriptPrelude + `
if not current(ARGV[1], ARGV[2], now()) then
    return 0
end
remove(ARGV[1], ARGV[4], ARGV[8])
return 1
`)

var settleScript = valkey.NewLuaScript(scriptPrelude + `
local id = ARGV[1]
local time = now()
if not current(id, ARGV[2], time) then
    return 0
end
if ARGV[9] == '1' or #ARGV[5] > maxMessageBytes then
    return 3
end
local delta = #ARGV[5] - redis.call('HSTRLEN', messages, id)
if used_bytes() + delta > maxBytes then
    return 4
end
redis.call('HSET', messages, id, ARGV[5])
redis.call('HSET', delays, id, ARGV[6])
adjust_bytes(delta)
if ARGV[3] == 'retry' then
    redis.call('ZADD', pending, time + tonumber(ARGV[7]), id)
else
    local expires = time + retention
    redis.call('ZADD', dead, expires, id)
    redis.call('ZADD', deadExpiry, expires, ARGV[8] .. ':' .. id)
end
redis.call('ZREM', inflight, id)
redis.call('HDEL', tokens, id)
return 1
`)

var extendScript = valkey.NewLuaScript(scriptPrelude + `
local time = now()
if not current(ARGV[1], ARGV[2], time) then
    return 0
end
redis.call('ZADD', inflight, time + tonumber(ARGV[3]), ARGV[1])
return 1
`)

var redriveScript = valkey.NewLuaScript(scriptPrelude + `
local previous = redis.call('GET', KEYS[13])
if previous then
    return tonumber(previous)
end
local time = now()
prune_queue(time, ARGV[4], ARGV[3], 128)
-- Exclude every expired dead letter, even when more than the bounded prune
-- limit remain. Retention expiry must never turn into redelivery.
local ids = redis.call('ZRANGEBYSCORE', dead, '(' .. string.format('%.6f', time), '+inf', 'LIMIT', 0, tonumber(ARGV[1]))
for _, id in ipairs(ids) do
    if redis.call('HEXISTS', messages, id) == 0 then
        return redis.error_reply('hypercube queue: missing dead-letter record')
    end
end
for _, id in ipairs(ids) do
    redis.call('HSET', attempts, id, 0)
    redis.call('HSET', delays, id, 0)
    redis.call('HDEL', tokens, id)
    redis.call('ZADD', pending, time, id)
    redis.call('ZREM', dead, id)
    redis.call('ZREM', deadExpiry, ARGV[3] .. ':' .. id)
end
if #ids > 0 then
    redis.call('SET', KEYS[13], #ids, 'PX', ARGV[2])
end
return #ids
`)

var statsScript = valkey.NewLuaScript(freeingScriptFlags + scriptPrelude + `
local time = now()
prune_queue(time, ARGV[1], ARGV[2], 128)
local ready = redis.call('ZCOUNT', pending, '-inf', time)
local expired = redis.call('ZCOUNT', inflight, '-inf', time)
return {
    ready + expired,
    redis.call('ZCARD', pending) - ready,
    redis.call('ZCARD', inflight) - expired,
    redis.call('ZCOUNT', dead, '(' .. string.format('%.6f', time), '+inf')
}
`)

var queuesScript = valkey.NewLuaScript(freeingScriptFlags + scriptPrelude + `return redis.call('HKEYS', queues)`)

var expiredQueuesScript = valkey.NewLuaScript(freeingScriptFlags + scriptPrelude + `
local cutoff = now()
if tonumber(ARGV[1]) > 0 then
    cutoff = math.min(cutoff, tonumber(ARGV[1]))
end
local members = redis.call('ZRANGEBYSCORE', deadExpiry, '-inf', cutoff, 'LIMIT', 0, tonumber(ARGV[2]))
local result = {}
for _, member in ipairs(members) do
    local separator = string.find(member, ':', 1, true)
    if not separator then
        return redis.error_reply('hypercube queue: malformed expiry index')
    end
    table.insert(result, {string.sub(member, 1, separator - 1), string.sub(member, separator + 1)})
end
return {string.format('%.6f', cutoff), result}
`)

var pruneScript = valkey.NewLuaScript(freeingScriptFlags + scriptPrelude + `
local cutoff = math.min(tonumber(ARGV[1]), now())
local completed = 0
-- IDs were selected from the global expiry index. Recheck their current dead
-- state atomically: another worker may have pruned, redriven, or reused an ID.
for i = 4, #ARGV - 4 do
    local id = ARGV[i]
    local deadline = redis.call('ZSCORE', dead, id)
    local member = ARGV[3] .. ':' .. id
    if deadline and tonumber(deadline) <= cutoff then
        remove(id, ARGV[2], ARGV[3])
        completed = completed + 1
    elseif deadline then
        redis.call('ZADD', deadExpiry, tonumber(deadline), member)
    else
        redis.call('ZREM', deadExpiry, member)
    end
end
return completed
`)
