--[[
  slurmjobpacker - the placement module for job_submit.lua

  Load it from your cluster's own job_submit.lua and call sjp.place() where the
  partition (and node) should be chosen. It returns true if it placed the job,
  or false and a reason if not, leaving the job to you (its admin_comment says
  why); then your own rule applies:

      local ok, sjp = pcall(dofile, "/opt/slurmjobpacker/lua/sjp.lua")
      if not ok then sjp = nil end

      function slurm_job_submit(job_desc, part_list, submit_uid)
          -- ... your own rules ...
          if sjp and sjp.place(job_desc, submit_uid) then return slurm.SUCCESS end
          -- ... your fallback, e.g. slim or fat partitions by memory per CPU ...
          return slurm.SUCCESS
      end

  sjp.place() places a batch job only where it can start: when some node in a
  partition that can hold it has room for it right now. It then sets the job's
  partitions -- and, when the job can start at once, its node -- from the
  table sjpd keeps in /run/sjp, and marks it in admin_comment ("sjp:..."). A
  job asking for nodes (--nodelist, --exclude) is placed among those nodes, or,
  with [policy] user_nodes = "ignore", the request is dropped and it is placed
  like any other. It never rejects a job. It does not place (returns false,
  and marks the job "sjp:skipped=<reason>"):
    "no room"     no node that could take the job has room for it now
    "no table"    sjpd is not running, or the disable file exists
    "stale"       sjpd has not refreshed the table or free space recently
    "not batch"   an interactive allocation (salloc, srun)
    "gpu"         the job asks for GPUs
    "reservation" the job runs in a reservation
    "constraint"  the job asks for node features (--constraint)
    "no usable node in --nodelist", "no partition holds those nodes"
                  no node sjp places on fits the nodes the job asked for
    "multi-node"  the job asks for more than one node (-N 2 or more)
    "no memory"   --mem=0, all of a node's memory
    "error"       anything raised; the error is logged

  slurmctld loads this file once, with your job_submit.lua; after upgrading
  sjp, run `scontrol reconfigure` to load the new version.
--]]

local sjp = {}

-- Where sjpd writes (override only if sjp.toml moves them), and how much to log.
sjp.config = {
    table_path = "/run/sjp/policy.lua",    -- <state_dir>/policy.lua in sjp.toml
    disable_path = "/etc/sjp/disable",     -- disable_file in sjp.toml
    verbose = false,                       -- log each job's shape and node in detail
}

local cache = { body = nil, tbl = nil }

local function file_exists(p)
    local f = io.open(p, "r")
    if f then f:close(); return true end
    return false
end

-- Reload only when the file actually changed. sjpd rewrites it only when the
-- decision surface changes, so in the steady state this costs one stat().
local function load_table()
    if file_exists(sjp.config.disable_path) then return nil end
    local f = io.open(sjp.config.table_path, "r")
    if not f then return nil end
    local body = f:read("*a")
    f:close()
    if not body or #body == 0 then return nil end
    -- Cache on the body itself. sjpd rewrites the file only when the decisions
    -- change, so this compare succeeds almost every time and costs a few KB of
    -- string comparison; caching on length alone could collide.
    if cache.body == body and cache.tbl then return cache.tbl end
    local chunk = load(body, "sjp-policy", "t", {})
    if not chunk then return nil end
    local ok, tbl = pcall(chunk)
    if not ok or type(tbl) ~= "table" or type(tbl.t) ~= "table" then return nil end
    cache.body, cache.tbl = body, tbl
    return tbl
end

local function bucket(value, edges)
    local i = 0
    for k = 1, #edges do
        if value >= edges[k] then i = i + 1 else break end
    end
    return i
end

-- The bucket table cannot answer feasibility: its top shape buckets are
-- open-ended, so a set chosen for a 0.86 TB job can be handed to a 2.2 TB job
-- that none of its partitions can hold. Filter by the job's real size against
-- each partition's largest node, which sjpd emits alongside the table.
local function keep_feasible(parts, tbl, mem_mb, cpus)
    if type(tbl.cap) ~= "table" then return parts end
    local out, biggest, biggest_mem = {}, nil, -1
    for p, c in pairs(tbl.cap) do
        if c[2] > biggest_mem then biggest, biggest_mem = p, c[2] end
    end
    for p in string.gmatch(parts, "[^,]+") do
        local c = tbl.cap[p]
        if c then
            if cpus <= c[1] and mem_mb <= c[2] then out[#out + 1] = p end
        else
            out[#out + 1] = p            -- unknown partition: do not second-guess
        end
    end
    if #out > 0 then return table.concat(out, ",") end
    -- Nothing in the looked-up set can hold this job, because the set was chosen
    -- for a bucket representative smaller than the job actually is. Pruning
    -- cannot recover from that, so recompute feasibility over ALL partitions
    -- from the caps -- which is the one thing the plugin has enough information
    -- to do on its own.
    for p, c in pairs(tbl.cap) do
        if cpus <= c[1] and mem_mb <= c[2] then out[#out + 1] = p end
    end
    if #out > 0 then
        table.sort(out)
        return table.concat(out, ",")
    end
    -- Genuinely fits nowhere. Name the roomiest partition anyway, so the user
    -- gets Slurm's ordinary "node configuration is not available" rather than a
    -- job with no partition at all.
    return biggest
end

local function packed_choice(mem_mb, cpus, minutes)
    local tbl = load_table()
    if not tbl then return nil, "no table" end
    if tbl.generated_at and tbl.max_age and
       (os.time() - tbl.generated_at) > tbl.max_age then
        return nil, "stale"
    end
    local i = bucket(mem_mb / cpus, tbl.mpc_edges)
    local j = bucket(cpus, tbl.cpu_edges)
    local k = bucket(minutes / 60, tbl.wt_edges)
    local key = string.format("%d,%d,%d", i, j, k)
    local parts = tbl.t[key]
    if type(parts) ~= "string" or parts == "" then return nil, "no entry" end
    local kept = keep_feasible(parts, tbl, mem_mb, cpus)
    if type(kept) ~= "string" or kept == "" then return nil, "infeasible" end
    local note = ""
    if kept ~= parts then note = " refit" end
    return kept, string.format("v%d b%s%s", tbl.version or 0, key, note), tbl, parts, key
end

-- ---------------------------------------------------------------- node pins
-- Mirrors sjp.policy.phi_node and pick_node. Keep them in step.
local function phi(fc, fm, demand)
    local s = 0
    for _, d in ipairs(demand) do s = s + d[2] * math.min(fc, fm / d[1]) end
    return s
end

-- Slurm tries a job's partitions in PriorityTier order and starts it in the
-- first with room, so the node is chosen inside the highest-ranked allowed
-- partition that has room: of the nodes destroying the least placeable capacity
-- (within min_gain), the one whose free memory per CPU is closest to the job's.
-- Mirrors sjp.policy.time_cost: how far the job would outlast the jobs already
-- on the node, as a log ratio; 0 unless [pin] time_aware is on.
-- Jobs this plugin pinned in the last 30 s: node -> {until, pinned at}. sjpd
-- reads the queue only every few seconds, so its busy_until can miss them.
local recent = {}

local function busy_until(pin, name)
    local t = (type(pin.busy_until) == "table" and pin.busy_until[name]) or 0
    local r = recent[name]
    if r and os.time() - r[2] < 30 then t = math.max(t, r[1]) end
    return t
end

local function time_cost(pin, name, minutes)
    if not pin.time_aware or type(pin.busy_until) ~= "table" or not minutes then return 0 end
    local busy = busy_until(pin, name) - os.time()
    return math.max(0, math.log(minutes * 60 / math.max(busy, 60)))
end

-- The pins this plugin made that sjpd's free space does not include yet:
-- {node, cpus, mem, pinned at}. sjpd counts a pending pinned job against its
-- node from its next read of the queue (pin.queue_at) on; until then, only
-- this list does. Without it, a burst of submissions within a few seconds
-- would all be pinned to the same free space.
local claims = {}

local function claimed(pin, name)
    local c, m, seen = 0, 0, (pin.queue_at or 0)
    for _, k in ipairs(claims) do
        if k[1] == name and k[4] >= seen then c, m = c + k[2], m + k[3] end
    end
    return c, m
end

local function claim(pin, name, cpus, mem)
    local keep, seen = {}, (pin.queue_at or 0)
    for _, k in ipairs(claims) do          -- drop the ones sjpd has seen since
        if k[4] >= seen and os.time() - k[4] < 300 then keep[#keep + 1] = k end
    end
    keep[#keep + 1] = { name, cpus, mem, os.time() }
    claims = keep
end

-- Mirrors sjp.policy.free_for: a node's free CPUs and memory, less the pins
-- sjpd has not seen yet and the reservations that start before a job of this
-- time limit would end (all of them when minutes is nil, a job without a
-- limit). sjpd has already taken out the reservations that are active now.
-- Below zero where jobs wait pinned to a node with no room yet.
local function free_raw(pin, name, minutes)
    local n = pin.nodes[name]
    local cc, cm = claimed(pin, name)
    local fc, fm = n[1] - cc, n[2] - cm
    local later = type(pin.later) == "table" and pin.later[name]
    if later then
        local ends = minutes and (os.time() + minutes * 60) or math.huge
        for _, r in ipairs(later) do
            if r[1] < ends then fc, fm = fc - r[2], fm - r[3] end
        end
    end
    return fc, fm
end

local function free_of(pin, name, minutes)
    local fc, fm = free_raw(pin, name, minutes)
    return math.max(fc, 0), math.max(fm, 0)
end

-- Mirrors sjp.policy.soonest_node: for a job no node has room for now, the
-- node expected to have room for it first -- its free space (the jobs waiting
-- there count against it) plus what its running jobs release by their time
-- limits. Ties go to the higher PriorityTier, then the name.
local function soonest_node(pin, parts, cpus, mem, minutes)
    if type(pin.ends) ~= "table" then return nil, "no end times" end
    local allowed = {}
    for p in string.gmatch(parts, "[^,]+") do allowed[p] = true end
    local now, best = os.time(), nil
    for name, n in pairs(pin.nodes) do
        local tier, ps = nil, {}
        for p in string.gmatch(n[3], "[^,]+") do
            if allowed[p] then
                ps[#ps + 1] = p
                local t = pin.tier[p] or 1
                if tier == nil or t > tier then tier = t end
            end
        end
        if tier then
            local fc, fm = free_raw(pin, name, minutes)
            local at = nil
            if fc >= cpus and fm >= mem then at = now end
            for _, e in ipairs(pin.ends[name] or {}) do
                if at then break end
                fc, fm = fc + e[2], fm + e[3]
                if fc >= cpus and fm >= mem then at = math.max(e[1], now) end
            end
            if at and (best == nil or at < best[1] or (at == best[1] and (tier > best[2]
               or (tier == best[2] and name < best[3])))) then
                table.sort(ps)
                best = { at, tier, name, table.concat(ps, ",") }
            end
        end
    end
    if not best then return nil, "no node there can ever hold it" end
    return best[3], best[4], best[1] - now
end

local function pick_node(pin, parts, cpus, mem, minutes)
    local allowed, ranks, seen = {}, {}, {}
    for p in string.gmatch(parts, "[^,]+") do
        allowed[p] = true
        local t = pin.tier[p] or 1
        if not seen[t] then seen[t] = true; ranks[#ranks + 1] = t end
    end
    table.sort(ranks, function(a, b) return a > b end)
    for _, r in ipairs(ranks) do
        local cands = {}
        for name, n in pairs(pin.nodes) do
            local fc, fm = free_of(pin, name, minutes)
            if fc >= cpus and fm >= mem then
                local hit = false
                for p in string.gmatch(n[3], "[^,]+") do
                    if allowed[p] and (pin.tier[p] or 1) == r then hit = true end
                end
                if hit then
                    cands[#cands + 1] = { phi(fc, fm, pin.demand)
                                          - phi(fc - cpus, fm - mem, pin.demand), name }
                end
            end
        end
        if #cands > 0 then
            if #cands == 1 then return nil, "only one node has room, so Slurm picks it anyway" end
            local want = mem / cpus
            local function mismatch(name)
                local fc, fm = free_of(pin, name, minutes)
                return math.abs(math.log((fm / fc) / want)) + time_cost(pin, name, minutes)
            end
            local lo, hi, worst_mis = math.huge, -math.huge, -math.huge
            for _, c in ipairs(cands) do
                c[3] = mismatch(c[2])
                if c[1] < lo then lo = c[1] end
                if c[1] > hi then hi = c[1] end
                if c[3] > worst_mis then worst_mis = c[3] end
            end
            local win = nil
            for _, c in ipairs(cands) do
                if c[1] - lo < pin.min_gain and (win == nil
                   or c[3] < win[3] or (c[3] == win[3] and (c[1] < win[1]
                   or (c[1] == win[1] and c[2] < win[2])))) then
                    win = c
                end
            end
            if hi - win[1] < pin.min_gain and worst_mis - win[3] < pin.min_ratio_gain then
                return nil, "the nodes with room are about equally good"
            end
            local best, ps = win[2], {}
            for p in string.gmatch(pin.nodes[best][3], "[^,]+") do
                if allowed[p] then ps[#ps + 1] = p end
            end
            table.sort(ps)
            return best, table.concat(ps, ",")
        end
    end
    return nil, "no room"
end

local function blank(v) return v == nil or v == "" end

-- Pin only plain jobs that can start the moment they are submitted.
local function pin_eligible(job_desc, submit_uid, cpus, tbl, mem_given)
    local pin = tbl.pin
    if type(pin) ~= "table" or not pin.enabled then return false, "pinning is off" end
    -- Without a memory request Slurm applies its own default, which sjp cannot
    -- see, so it cannot know whether the job fits the node.
    if not mem_given then return false, "no memory request" end
    if type(pin.nodes) ~= "table" then return false, "no node data" end
    if not tbl.generated_at or (os.time() - tbl.generated_at) > pin.max_age then
        return false, "node data too old"
    end
    if not blank(job_desc.req_nodes) then return false, "--nodelist" end
    -- A job with a dependency is treated like any other: if it has not started
    -- by [pin] release_after, sjpd releases the pin.
    if not blank(job_desc.array_inx) then return false, "array" end
    if not blank(job_desc.admin_comment) then return false, "has an admin comment" end
    if not blank(job_desc.tres_per_node) then return false, "--gres" end
    if job_desc.min_nodes and job_desc.min_nodes ~= slurm.NO_VAL
       and job_desc.min_nodes > 1 then return false, "several nodes" end
    -- --nodelist means "include this node", not "only this node": a job of
    -- several tasks that may span nodes would be split around the pin.
    local tasks = job_desc.num_tasks
    if tasks and tasks ~= slurm.NO_VAL and tasks > 1 and job_desc.max_nodes ~= 1 then
        return false, "several tasks without -N 1"
    end
    if job_desc.begin_time and job_desc.begin_time > os.time() then
        return false, "--begin in the future"
    end
    if job_desc.priority == 0 then return false, "held" end
    if job_desc.shared == 0 then return false, "--exclusive" end
    -- At the per-user CPU cap of its QOS it would not start whichever node it
    -- got. Without --qos, assume the QOS the user's running jobs are in.
    if type(pin.room) == "table" then
        local uid = math.floor(submit_uid)
        local qos = job_desc.qos
        if blank(qos) and type(pin.user_qos) == "table" then qos = pin.user_qos[uid] end
        local left = qos and pin.room[qos] and pin.room[qos][uid]
        if left and left < cpus then
            return false, string.format("user at the CPU cap of QOS %s (%d CPUs left)", qos, left)
        end
    end
    return true
end

-- Memory the job asks for, per node, in MB, read without changing the job; and
-- whether it asked at all. nil for --mem=0 (all of a node's memory).
local function job_mem(job_desc, cpus)
    local per_cpu, per_node = job_desc.min_mem_per_cpu, job_desc.min_mem_per_node
    if per_cpu and per_cpu ~= slurm.NO_VAL64 and per_cpu > 0 then
        return per_cpu * cpus, true
    end
    if per_node == 0 then return nil, true end
    if per_node and per_node ~= slurm.NO_VAL64 then return per_node, true end
    return 512, false
end

local function has_gpu(job_desc)
    for _, f in ipairs({ "tres_per_node", "tres_per_job", "tres_per_task",
                         "tres_per_socket" }) do
        local v = job_desc[f]
        if v and string.find(string.lower(v), "gpu") then return true end
    end
    return false
end

-- Whether some node in the given partitions has room for the job right now,
-- from the free space sjpd publishes with the table; nil if that is too old.
local function has_room(tbl, parts, cpus, mem, minutes)
    local pin = tbl.pin
    if type(pin) ~= "table" or type(pin.nodes) ~= "table" then return true end
    if (os.time() - (tbl.generated_at or 0)) > (pin.max_age or 10) then return nil end
    local allowed = {}
    for p in string.gmatch(parts, "[^,]+") do allowed[p] = true end
    for name, n in pairs(pin.nodes) do
        local fc, fm = free_of(pin, name, minutes)
        if fc >= cpus and fm >= mem then
            for p in string.gmatch(n[3], "[^,]+") do
                if allowed[p] then return true end
            end
        end
    end
    return false
end

-- MB as GB, rounded; one decimal below 10 GB, so 512 MB is 0.5G, not 0G.
local function gb(mb)
    local g = mb / 1024
    if g < 10 then return string.format("%.1fG", g) end
    return string.format("%.0fG", g)
end

-- Seconds as hours, rounded the same way.
local function hours(sec)
    local h = sec / 3600
    if h < 10 then return string.format("%.1fh", h) end
    return string.format("%.0fh", h)
end

local function mpc(cpus, mem)
    if cpus <= 0 then return "-" end
    return string.format("%.0f", mem / cpus)
end

-- "n[01-03],gpu1" -> {"n01", "n02", "n03", "gpu1"}
local function expand(list)
    local out = {}
    for head, ranges, tail in string.gmatch(list .. ",", "([^,%[]+)%[?([^%]]*)%]?([^,]*),") do
        if ranges == "" then
            out[#out + 1] = head .. tail
        else
            for r in string.gmatch(ranges, "[^,]+") do
                local a, b = string.match(r, "^(%d+)%-?(%d*)$")
                if a then
                    for i = tonumber(a), tonumber(b ~= "" and b or a) do
                        out[#out + 1] = string.format("%s%0" .. #a .. "d%s", head, i, tail)
                    end
                end
            end
        end
    end
    return out
end

-- Mirrors sjp.policy.eligible and restrict. The nodes a job may use: those it
-- asked for (--nodelist), less those it excluded (--exclude); as a view of pin
-- with only those nodes. Then its partitions narrowed to those holding such
-- nodes -- every node it named, since Slurm requires each of its partitions
-- to contain them.
local function eligible(pin, job_desc)
    local want, skip, named = nil, {}, 0
    if not blank(job_desc.req_nodes) then
        want = {}
        for _, n in ipairs(expand(job_desc.req_nodes)) do want[n] = true; named = named + 1 end
    end
    if not blank(job_desc.exc_nodes) then
        for _, n in ipairs(expand(job_desc.exc_nodes)) do skip[n] = true end
    end
    if not want and not next(skip) then return pin, nil, 0 end
    local nodes = {}
    for name, n in pairs(pin.nodes) do
        if (not want or want[name]) and not skip[name] then nodes[name] = n end
    end
    if not next(nodes) then return nil, "no usable node in --nodelist" end
    return setmetatable({ nodes = nodes }, { __index = pin }), want, named
end

local function restrict(parts, pin, want, named)
    local held = {}
    for name, n in pairs(pin.nodes) do
        for p in string.gmatch(n[3], "[^,]+") do held[p] = (held[p] or 0) + 1 end
    end
    local ok = function(p) return held[p] and (not want or held[p] >= named) end
    local out = {}
    for p in string.gmatch(parts, "[^,]+") do if ok(p) then out[#out + 1] = p end end
    if #out == 0 then
        for p in pairs(held) do if ok(p) then out[#out + 1] = p end end
        table.sort(out)
    end
    if #out == 0 then return nil end
    return table.concat(out, ",")
end

-- place() returns placed, why, the details the verbose log adds, and the job's
-- shape for the mark a job sjp skips gets.
local function place(job_desc, submit_uid)
    if not job_desc.script or job_desc.script == "" then return false, "not batch" end
    if has_gpu(job_desc) then return false, "gpu" end
    if not blank(job_desc.reservation) then return false, "reservation" end
    -- Node features (--constraint) are the user's way to ask for particular
    -- hardware; sjp stands aside.
    if not blank(job_desc.features) then return false, "constraint" end
    -- sjp sizes a job against single nodes; one spanning several is the site's.
    local nodes = job_desc.min_nodes
    if nodes and nodes ~= slurm.NO_VAL and nodes > 1 then return false, "multi-node" end

    local cpus = job_desc.min_cpus
    if not cpus or cpus == 0 or cpus == slurm.NO_VAL then cpus = 1 end
    local mem, mem_given = job_mem(job_desc, cpus)
    if not mem then return false, "no memory" end
    local minutes = job_desc.time_limit
    local has_limit = true
    if not minutes or minutes == slurm.NO_VAL or minutes == slurm.INFINITE then
        minutes, has_limit = 60, false
    end
    local detail = string.format("job %d CPUs x %d MB (%s MB/CPU%s), %d min",
                                 cpus, mem, mpc(cpus, mem),
                                 mem_given and "" or ", no memory request", minutes)

    local shape = string.format("job=%dc,%s,%s/c", cpus, gb(mem), gb(mem / cpus))
    local parts, why, tbl, looked_up, key = packed_choice(mem, cpus, minutes)
    if not parts then return false, why, detail, shape end
    detail = detail .. "; table " .. looked_up
    if parts ~= looked_up then detail = detail .. ", refit to " .. parts end
    -- The nodes it may use (--nodelist, --exclude), and the partitions holding
    -- them; all decisions below see only those nodes. With [policy] user_nodes
    -- = "ignore" the request is dropped once the job is placed.
    local own, want, named, narrowed = tbl.pin, nil, 0, false
    local drop_nodes = tbl.user_nodes == "ignore"
    if drop_nodes then
        -- placed like any other job
    elseif type(tbl.pin) == "table" and type(tbl.pin.nodes) == "table" then
        own, want, named = eligible(tbl.pin, job_desc)
        if not own then return false, want, detail, shape end
        narrowed = own ~= tbl.pin
        if narrowed then
            parts = restrict(parts, own, want, named)
            if not parts then return false, "no partition holds those nodes", detail, shape end
            detail = detail .. ", narrowed to " .. parts
            tbl = setmetatable({ pin = own }, { __index = tbl })
        end
    end
    local room = has_room(tbl, parts, cpus, mem, has_limit and minutes or nil)
    if room == nil then return false, "stale", detail, shape end
    local by_shape = false
    if not room then
        if not tbl.always or type(tbl.tf) ~= "table" or type(tbl.tf[key]) ~= "string" then
            return false, "no room", detail .. "; no node there has the CPUs and memory free", shape
        end
        -- [policy] always_place: where the job's shape fits best on an empty cluster
        parts = keep_feasible(tbl.tf[key], tbl, mem, cpus)
        if narrowed then parts = restrict(parts, own, want, named) or parts end
        by_shape = true
        why = why .. " no room, by shape"
        detail = detail .. "; no node there has room now, so by shape: " .. parts
    end

    job_desc.partition = parts
    if drop_nodes and not (blank(job_desc.req_nodes) and blank(job_desc.exc_nodes)) then
        detail = detail .. "; dropped the job's own --nodelist/--exclude (user_nodes = ignore)"
        job_desc.req_nodes, job_desc.exc_nodes = "", ""
    end
    -- What sjp did, for `scontrol show job`: the job's shape and the table
    -- version, and for a pin the node's free space when it was chosen.
    local version = string.format("v=%d", tbl.version or 0)
    local mark = "sjp:placed=" .. parts .. ";" .. shape .. ";" .. version
    if by_shape then mark = mark .. ";room=no" end
    -- Pin the node when the job can start now; with [pin] wait_for_room, also
    -- when it cannot, to the node expected to have room first. Any failure
    -- here leaves the partition choice above as it is.
    local eta = nil
    local pok, node, pparts = pcall(function()
        local ok_pin, no = pin_eligible(job_desc, submit_uid, cpus, tbl, mem_given)
        if not ok_pin then return nil, no end
        if not by_shape then
            return pick_node(tbl.pin, parts, cpus, mem, has_limit and minutes or nil)
        end
        if not tbl.pin.wait then return nil, "no room, and wait_for_room is off" end
        local n, ps, e = soonest_node(tbl.pin, parts, cpus, mem, has_limit and minutes or nil)
        eta = e
        return n, ps
    end)
    if pok and node and pparts then
        job_desc.req_nodes = node
        job_desc.partition = pparts
        local fc, fm = free_of(tbl.pin, node, has_limit and minutes or nil)
        mark = string.format("sjp:pin=%s;from=%s;%s;free=%dc,%s,%s/c;%s", node, parts,
                             shape, fc, gb(fm), gb(fm / math.max(fc, 1)), version)
        if eta then mark = mark .. ";room=no;eta=" .. hours(eta) end
        if tbl.pin.time_aware and type(tbl.pin.busy_until) == "table" then
            -- the job's time limit, and how long the node's jobs still ran
            local until_ = busy_until(tbl.pin, node)
            mark = mark .. ";time=" .. hours(minutes * 60)
                        .. ";busy=" .. hours(math.max(0, until_ - os.time()))
            if has_limit then
                recent[node] = { math.max(until_, os.time() + minutes * 60), os.time() }
            end
        end
        if eta then
            detail = detail .. string.format("; waits for node %s, room expected in %s",
                                             node, hours(eta))
        else
            detail = detail .. string.format(
                "; node %s free %d CPUs x %d MB (%s MB/CPU), after %d x %d (%s MB/CPU)",
                node, fc, fm, mpc(fc, fm), fc - cpus, fm - mem, mpc(fc - cpus, fm - mem))
        end
        claim(tbl.pin, node, cpus, mem)         -- counted until sjpd has seen it
        why = why .. " pin=" .. node .. (eta and (" eta " .. hours(eta)) or "")
    else
        detail = detail .. "; no pin: " .. tostring(pok and pparts or node or "")
    end
    if blank(job_desc.admin_comment) then job_desc.admin_comment = mark end
    return true, why, detail
end

-- Place a batch job where it fits now. Returns true, or false and a reason
-- (see the top of this file); a job that is not placed keeps its partitions
-- and nodes, and gets "sjp:skipped=<reason>" if it has no admin_comment.
function sjp.place(job_desc, submit_uid)
    local ok, placed, why, detail, shape = pcall(place, job_desc, submit_uid)
    if not ok then
        slurm.log_error("sjp: %s", tostring(placed))
        return false, "error"
    end
    -- Not placed: say why on the job, for `scontrol show job` and for sjpd,
    -- which takes over the ones skipped for want of room once room opens.
    if not placed and blank(job_desc.admin_comment) then
        job_desc.admin_comment = "sjp:skipped=" .. tostring(why) .. (shape and (";" .. shape) or "")
    end
    local line = string.format("sjp: uid=%.0f name='%s' -> %s (%s)", submit_uid,
                               job_desc.name or "?",
                               placed and job_desc.partition or "not placed", tostring(why))
    if sjp.config.verbose and detail then line = line .. ": " .. detail end
    slurm.log_info("%s", line)
    return placed, why
end

return sjp
