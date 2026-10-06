-- Runs lua/sjp.lua outside Slurm, for tests/test_lua.py.
--   lua5.4 tests/lua_harness.lua <sjp.lua> <policy.lua> <disable file> <jobs.lua>
-- jobs.lua returns a list of {id = ..., fresh = bool, job = {job_desc fields}}.
-- A job with fresh = true gets a newly loaded sjp.lua (no pins remembered);
-- the others share the one before them, as consecutive submissions do.
-- Prints one line per job: id|placed|partition|req_nodes|admin_comment|why

local sjp_path, table_path, disable_path, jobs_path = arg[1], arg[2], arg[3], arg[4]

slurm = {
    NO_VAL = 4294967294, NO_VAL64 = 0xfffffffffffffffe, INFINITE = 0xffffffff,
    SUCCESS = 0,
    log_info = function() end, log_error = function(f, ...) io.stderr:write(string.format(f, ...), "\n") end,
    log_user = function() end,
}

local function load_sjp()
    local sjp = dofile(sjp_path)
    sjp.config.table_path, sjp.config.disable_path = table_path, disable_path
    return sjp
end

local sjp = nil
for _, case in ipairs(dofile(jobs_path)) do
    if case.fresh or not sjp then sjp = load_sjp() end
    local job = case.job
    local placed, why = sjp.place(job, case.uid or 1000)
    print(table.concat({ case.id, tostring(placed), job.partition or "", job.req_nodes or "",
                         job.admin_comment or "", tostring(why) }, "|"))
end
