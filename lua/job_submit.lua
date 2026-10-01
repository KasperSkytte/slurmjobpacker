--[[
  A default job_submit.lua for clusters that do not have one yet.

  If your cluster already has a job_submit.lua, keep it and add sjp to it
  instead: load lua/sjp.lua once at the top, and call sjp.place() where your
  script chooses a batch job's partition (see the README).

  This one:
    - sends interactive allocations (salloc, srun: no job script) to the
      INTERACTIVE partition, if the cluster has one the user may use;
    - optionally sends GPU jobs to GPU_PARTITION;
    - lets sjp place everything else;
    - and when sjp does not place a batch job (no node has room for it now,
      or sjpd is not running), optionally applies a fixed rule: SLIM below
      FAT_ABOVE MB of memory per CPU, FAT above it.

  Copy it next to slurm.conf, set JobSubmitPlugins=lua there, and run
  `scontrol reconfigure`.
--]]

local SJP = "/opt/slurmjobpacker/lua/sjp.lua"

local INTERACTIVE = "interactive"    -- partition for salloc/srun; "" leaves them alone
local INTERACTIVE_QOS = ""           -- QOS for them too, e.g. "interactive"; "" keeps theirs
local GPU_PARTITION = ""             -- partition for GPU jobs, e.g. "gpu"; "" leaves them alone
local SLIM = ""                      -- fallback partitions, e.g. "slim1,slim2"; "" leaves the
local FAT = ""                       -- job as submitted, e.g. "fat1,fat2"
local FAT_ABOVE = 6000               -- MB per CPU from which a job counts as FAT

local ok, sjp = pcall(dofile, SJP)
if not ok then
    slurm.log_error("job_submit: cannot load %s: %s", SJP, tostring(sjp))
    sjp = nil
end
if sjp then                          -- settings: see sjp.config in lua/sjp.lua
    -- sjp.config.verbose = true     -- log each job's shape and node in detail
end

local function wants_gpu(job_desc)
    for _, f in ipairs({ "tres_per_node", "tres_per_job", "tres_per_task" }) do
        local v = job_desc[f]
        if v and string.find(string.lower(v), "gpu") then return true end
    end
    return false
end

function slurm_job_submit(job_desc, part_list, submit_uid)
    local batch = job_desc.script and job_desc.script ~= ""

    if not batch then
        if INTERACTIVE ~= "" and part_list[INTERACTIVE] then
            job_desc.partition = INTERACTIVE
            if INTERACTIVE_QOS ~= "" then job_desc.qos = INTERACTIVE_QOS end
        end
        return slurm.SUCCESS
    end

    if GPU_PARTITION ~= "" and wants_gpu(job_desc) then
        job_desc.partition = GPU_PARTITION
        return slurm.SUCCESS
    end

    -- batch jobs do not belong in the interactive partition
    if INTERACTIVE ~= "" and job_desc.partition == INTERACTIVE then
        slurm.log_user("Batch jobs are not allowed in the '%s' partition; choosing another.",
                       INTERACTIVE)
        job_desc.partition = ""          -- "" clears it; nil would raise
    end

    if sjp and sjp.place(job_desc, submit_uid) then return slurm.SUCCESS end

    -- sjp did not place it: the fixed rule, if set
    if SLIM ~= "" and FAT ~= "" then
        local cpus = job_desc.min_cpus
        if not cpus or cpus == 0 or cpus == slurm.NO_VAL then cpus = 1 end
        local per_cpu = job_desc.min_mem_per_cpu
        if not per_cpu or per_cpu == slurm.NO_VAL64 then
            local per_node = job_desc.min_mem_per_node
            per_cpu = (per_node and per_node ~= slurm.NO_VAL64) and per_node / cpus or 0
        end
        job_desc.partition = (per_cpu >= FAT_ABOVE) and FAT or SLIM
    end
    return slurm.SUCCESS
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
    return slurm.SUCCESS
end

return slurm.SUCCESS
