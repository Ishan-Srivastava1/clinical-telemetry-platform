-- KEYS[1]  sliding-window sorted set key (e.g. "tel:HR:P0001")
-- ARGV[1]  timestamp ms (also the score for ZADD)
-- ARGV[2]  value
-- ARGV[3]  window size in ms (e.g. 30000)
-- ARGV[4]  z-score threshold (e.g. 3.5)
-- ARGV[5]  minimum window count before z-score is trusted (e.g. 10)
--
-- Returns a JSON object:
--   { flagged, reason, n, mean, stddev, z }
--
-- Atomic: redis runs each EVAL on a single thread, no other client can
-- interleave between the evict / add / read steps.

local key       = KEYS[1]
local ts        = tonumber(ARGV[1])
local value     = tonumber(ARGV[2])
local window_ms = tonumber(ARGV[3])
local z_thresh  = tonumber(ARGV[4])
local min_count = tonumber(ARGV[5])

-- 1) Evict expired entries
local cutoff = ts - window_ms
redis.call('ZREMRANGEBYSCORE', key, '-inf', '(' .. cutoff)

-- 2) Append new sample. Encode "<value>:<ts>" so identical values across
--    timestamps don't deduplicate inside the sorted set.
local member = tostring(value) .. ':' .. tostring(ts)
redis.call('ZADD', key, ts, member)

-- 3) Refresh TTL (slightly longer than the window so an idle key auto-evicts)
redis.call('PEXPIRE', key, window_ms * 2)

-- 4) Pull all surviving members and compute running statistics
local members = redis.call('ZRANGE', key, 0, -1)
local n = #members
local sum = 0
local sum_sq = 0
for i = 1, n do
    local v_str = string.match(members[i], "([^:]+):")
    local v = tonumber(v_str)
    sum = sum + v
    sum_sq = sum_sq + (v * v)
end

-- 5) Warmup: not enough history to compute a meaningful z-score
if n < min_count then
    return cjson.encode({
        flagged = false,
        reason  = "warmup",
        n       = n,
        mean    = (n > 0) and (sum / n) or 0,
        stddev  = 0,
        z       = 0
    })
end

local mean = sum / n
local variance = (sum_sq / n) - (mean * mean)
if variance < 0 then variance = 0 end   -- guard against floating-point drift
local stddev = math.sqrt(variance)

local z
if stddev > 0 then
    z = (value - mean) / stddev
else
    z = 0
end

local flagged = (math.abs(z) > z_thresh)

return cjson.encode({
    flagged = flagged,
    reason  = flagged and "outlier" or "ok",
    n       = n,
    mean    = mean,
    stddev  = stddev,
    z       = z
})