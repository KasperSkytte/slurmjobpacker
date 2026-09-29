--[[
  slurmqueuepacker - the placement module for job_submit.lua

  Load it from your cluster's own job_submit.lua and call sqp.place() where the
  partition (and node) should be chosen:

      local sqp = dofile("/opt/slurmqueuepacker/lua/sqp.lua")

      function slurm_job_submit(job_desc, part_list, submit_uid)
          -- ... your own rules ...
          return sqp.place(job_desc, submit_uid)
      end

  sqp.place() looks the job up in the table sqpd keeps in /run/sqp and sets its
  partitions -- and, when it can start at once, its node. It only ever changes
  job_desc.partition, and for a pinned job req_nodes and admin_comment; it never
  rejects a job and always returns slurm.SUCCESS. It leaves alone interactive
  allocations (no job script), GPU jobs and jobs in a reservation. If the table
  is missing, stale or unparsable, the disable file exists, or anything raises,
  the job gets the fallback in sqp.config, or keeps its partition if there is none.

  slurmctld loads this file once, with your job_submit.lua; after upgrading
  sqp, run `scontrol reconfigure` to load the new version.
--]]

local sqp = {}

-- Override any of these after loading, e.g. sqp.config.slim = "slim1,slim2".
sqp.config = {
    table_path = "/run/sqp/policy.lua",    -- <state_dir>/policy.lua in sqp.toml
    disable_path = "/etc/sqp/disable",     -- disable_file in sqp.toml
    -- Fallback when sqp has no table: jobs asking less than ratio_threshold MB
    -- per CPU get slim, the rest fat (comma-separated partition lists). Empty:
    -- the job keeps the partition it has.
    slim = "",
    fat = "",
    ratio_threshold = 6000,
    -- true: sqp places every batch job, including ones submitted with
    -- --partition (users often choose wrongly). false: those keep theirs.
    override_user_partition = true,
}

local cache = { body = nil, tbl = nil }

local function file_exists(p)
    local f = io.open(p, "r")
    if f then f:close(); return true end
    return false
end

-- Reload only when the file actually changed. sqpd rewrites it only when the
-- decision surface changes, so in the steady state this costs one stat().
local function load_table()
    if file_exists(sqp.config.disable_path) then return nil end
    local f = io.open(sqp.config.table_path, "r")
    if not f then return nil end
    local body = f:read("*a")
    f:close()
    if not body or #body == 0 then return nil end
    -- Cache on the body itself. sqpd rewrites the file only when the decisions
    -- change, so this compare succeeds almost every time and costs a few KB of
    -- string comparison; caching on length alone could collide.
    if cache.body == body and cache.tbl then return cache.tbl end
    local chunk = load(body, "sqp-policy", "t", {})
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

local function static_choice(mem_mb, cpus)
    local c = sqp.config
    local parts = c.fat
    if (mem_mb / cpus) < c.ratio_threshold then parts = c.slim end
    if parts == "" then return nil end
    return parts
end

-- The bucket table cannot answer feasibility: its top shape buckets are
-- open-ended, so a set chosen for a 0.86 TB job can be handed to a 2.2 TB job
-- that none of its partitions can hold. Filter by the job's real size against
-- each partition's largest node, which sqpd emits alongside the table.
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
    local parts = tbl.t[string.format("%d,%d,%d", i, j, k)]
    if type(parts) ~= "string" or parts == "" then return nil, "no entry" end
    local kept = keep_feasible(parts, tbl, mem_mb, cpus)
    if type(kept) ~= "string" or kept == "" then return nil, "infeasible" end
    local note = ""
    if kept ~= parts then note = " refit" end
    return kept, string.format("v%d b%d,%d,%d%s", tbl.version or 0, i, j, k, note), tbl
end

-- ---------------------------------------------------------------- node pins
-- Mirrors sqp.policy.phi_node and pick_node. Keep them in step.
local function phi(fc, fm, demand)
    local s = 0
    for _, d in ipairs(demand) do s = s + d[2] * math.min(fc, fm / d[1]) end
    return s
end

-- Slurm tries a job's partitions in PriorityTier order and starts it in the
-- first with room, so the node is chosen inside the highest-ranked allowed
-- partition that has room: of the nodes destroying the least placeable capacity
-- (within min_gain), the one whose free memory per CPU is closest to the job's.
local function pick_node(pin, parts, cpus, mem)
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
            if n[1] >= cpus and n[2] >= mem then
                local hit = false
                for p in string.gmatch(n[3], "[^,]+") do
                    if allowed[p] and (pin.tier[p] or 1) == r then hit = true end
                end
                if hit then
                    cands[#cands + 1] = { phi(n[1], n[2], pin.demand)
                                          - phi(n[1] - cpus, n[2] - mem, pin.demand), name }
                end
            end
        end
        if #cands > 0 then
            if #cands == 1 then return nil, "one candidate" end
            local want = mem / cpus
            local function mismatch(name)
                local n = pin.nodes[name]
                return math.abs(math.log((n[2] / n[1]) / want))
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
                return nil, "equal"
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
    -- Without a memory request Slurm applies its own default, which sqp cannot
    -- see, so it cannot know whether the job fits the node.
    if not mem_given then return false end
    if type(pin) ~= "table" or type(pin.nodes) ~= "table" then return false end
    if not tbl.generated_at or (os.time() - tbl.generated_at) > pin.max_age then
        return false
    end
    if not blank(job_desc.req_nodes) or not blank(job_desc.exc_nodes) then return false end
    -- A job with a dependency is treated like any other: if it has not started
    -- by [pin] release_after, sqpd releases the pin.
    if not blank(job_desc.array_inx) then return false end
    if not blank(job_desc.features) or not blank(job_desc.admin_comment) then return false end
    if not blank(job_desc.tres_per_node) then return false end
    if job_desc.min_nodes and job_desc.min_nodes ~= slurm.NO_VAL
       and job_desc.min_nodes > 1 then return false end
    -- --nodelist means "include this node", not "only this node": a job of
    -- several tasks that may span nodes would be split around the pin.
    local tasks = job_desc.num_tasks
    if tasks and tasks ~= slurm.NO_VAL and tasks > 1 and job_desc.max_nodes ~= 1 then
        return false
    end
    if job_desc.begin_time and job_desc.begin_time > os.time() then return false end
    if job_desc.priority == 0 then return false end                  -- held
    if job_desc.shared == 0 then return false end                    -- --exclusive
    -- At the per-user CPU cap it would not start whichever node it got.
    if type(pin.room) == "table" and (blank(job_desc.qos) or job_desc.qos == pin.cap_qos) then
        local left = pin.room[math.floor(submit_uid)]
        if left and left < cpus then return false end
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

local function place(job_desc, submit_uid)
    -- Left alone: interactive allocations (salloc, srun), GPU jobs, jobs in a
    -- reservation, and, if so configured, jobs that chose their partition.
    if not job_desc.script or job_desc.script == "" then return end
    if has_gpu(job_desc) or not blank(job_desc.reservation) then return end
    if not sqp.config.override_user_partition and not blank(job_desc.partition) then return end

    local cpus = job_desc.min_cpus
    if not cpus or cpus == 0 or cpus == slurm.NO_VAL then cpus = 1 end
    local mem, mem_given = job_mem(job_desc, cpus)
    if not mem then return end
    local minutes = job_desc.time_limit
    if not minutes or minutes == slurm.NO_VAL or minutes == slurm.INFINITE then
        minutes = 60
    end

    local ok, parts, why, tbl = pcall(packed_choice, mem, cpus, minutes)
    if ok and parts then
        job_desc.partition = parts
        -- Pin the node only when the job can start now. Any failure here
        -- leaves the partition choice above untouched.
        local pok, node, pparts = pcall(function()
            if pin_eligible(job_desc, submit_uid, cpus, tbl, mem_given) then
                return pick_node(tbl.pin, parts, cpus, mem)
            end
        end)
        if pok and node and pparts then
            job_desc.req_nodes = node
            job_desc.partition = pparts
            job_desc.admin_comment = "sqp:pin=" .. node .. ";from=" .. parts
            -- Count it against the node until the next table arrives, so a burst
            -- of submissions is not all pinned to the same free space.
            local n = tbl.pin.nodes[node]
            n[1], n[2] = n[1] - cpus, n[2] - mem
            why = (why or "") .. " pin=" .. node
        end
        slurm.log_info("sqp: uid=%.0f name='%s' %dc %dMB -> %s (%s)",
                       submit_uid, job_desc.name or "?", cpus, mem,
                       job_desc.partition, why or "")
    else
        local fallback = static_choice(mem, cpus)
        if fallback then job_desc.partition = fallback end
        slurm.log_info("sqp: uid=%.0f name='%s' %dc %dMB -> %s (fallback: %s)",
                       submit_uid, job_desc.name or "?", cpus, mem,
                       job_desc.partition or "unchanged", tostring(parts or why))
    end
end

-- Choose the job's partitions (and node) in place. Never rejects the job.
function sqp.place(job_desc, submit_uid)
    local ok, err = pcall(place, job_desc, submit_uid)
    if not ok then slurm.log_error("sqp: %s", tostring(err)) end
    return slurm.SUCCESS
end

return sqp
