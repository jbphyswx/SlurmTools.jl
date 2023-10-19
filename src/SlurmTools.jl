module SlurmTools

using DataFrames, CSV, Dates


export sbatch, sacct, scancel, sbatch_julia_expr

"""
    sbatch(cmd[, options])

Submits the script (and any arguments) specified by `cmd` as a batch job to Slurm,
returning the job id. `options` is a collection of command-line options that might be
passed

# Example

```julia
julia> sbatch(`job.sh arg1`, ["--time=01:00:00", "--nprocs=4"])
"2332883"
```

# External links
- [`sbatch` man page](https://slurm.schedmd.com/sbatch.html)
"""
function sbatch(cmd, options=[],verbose=true)  
    if verbose
        println("--------------------------------------------------") 
    end
    # cmd = [cmd]

    if verbose 
        # sbatch_cmd = `sbatch $options $cmd`
        sbatch_cmd = "sbatch "*join(options," ")*" "*cmd # this might work if you make a string haha
    else # parsable doesnt seem to make a difference, nothing sems to print to the output file till the end of the run for some reason using this fcn...
        sbatch_cmd = "sbatch "*join(options," ")*" "*cmd # this might work if you make a string haha
        # sbatch_cmd = `sbatch --parsable $options $cmd` 
        # sbatch_cmd = "sbatch --parsable "*join(options," ")*" "*cmd # this might work if you make a string haha   
    end    

    if verbose
        println(sbatch_cmd); println("")
    end
    # run(sbatch_cmd)
    # run(`/bin/bash -c $sbatch_cmd`) # call a shell like https://stackoverflow.com/a/67448187 and it should work... (why did i switch to this one? doesn't return JOBID)
    return chomp(String(read(`/bin/bash -c $sbatch_cmd`)))
    return
    # return chomp(String(read(sbatch_cmd)))
    # return readchomp(sbatch_cmd) # the readchomp(x) command is equivalent to writing chomp(read(x, String)):



end



"""
    sacct([options])

Gets accounting data for all jobs and job steps, returning a DataFrame.

Do not use any options that change the formatting of the output of `sacct`
(e.g. `--parsable`, `--helpformat`, etc).

# Example

```
julia> sacct(["--format=JobID,Start,End", "--allocations"])
340×3 DataFrames.DataFrame
│ Row │ JobID      │ Start               │ End                 │
│     │ String     │ DateTime            │ DateTime            │
├─────┼────────────┼─────────────────────┼─────────────────────┤
│ 1   │ 8646713    │ 2020-01-30T00:02:13 │ 2020-01-30T00:08:48 │
│ 2   │ 8646714    │ 2020-01-30T00:08:48 │ 2020-01-30T00:10:07 │
...
```

# See also
- [`sacct_formats`](@ref)

# External links
- [`sacct` man page](https://slurm.schedmd.com/sacct.html)
"""
# function sacct(options=[])
#     delim = '\t'
#     sacct_cmd = `sacct --parsable2 --delimiter=$delim $options`
#     CSV.read(sacct_cmd;
#              delim=delim, type=String, types=DEFAULT_TYPES, missingstrings=["","Unknown"])
# end

function sacct(options=[],sink=DataFrame) # needs a valid sink or somethin, see https://discourse.julialang.org/t/csv-read-error-provide-a-valid-sink-argument/50157,
    delim = '\t'
    sacct_cmd = `sacct --parsable2 --delimiter=$delim $options`
    CSV.read(sacct_cmd, sink;
             delim=delim, types=default_type_func, missingstring=["","Unknown"])
end

# TODO: add more types here
# need CSV to support custom parsing for e.g. "Elapsed"
const DEFAULT_TYPES = Dict(
    "Submit" => DateTime,
    "Start" => DateTime,
    "Eligible" => DateTime,
    "End" => DateTime,
)



# default_type_func(i,name) = DataStructures.DefaultDict(String,DEFAULT_TYPES) # default to string, cause type default is deprecated, see https://github.com/JuliaData/CSV.jl/issues/575#issuecomment-1051339023
default_type_func(i,name) = name in keys(DEFAULT_TYPES) ? DEFAULT_TYPES[name] : String # default to string, cause type default is deprecated, see https://github.com/JuliaData/CSV.jl/issues/575#issuecomment-1051339023

"""
    sacct_formats()

Returns a list of commands that can be passed with the `"--format"` option to [`sacct`](@ref).
"""
function sacct_formats()
    split(String(read(`sacct --helpformat`)))
end


"""
    scancel(job)

Cancel a queued Slurm job.

# External links
- [`scancel` man page](https://slurm.schedmd.com/scancel.html)
"""
function scancel(job)
    run(`scancel $job`)
end




"""
take a current julia expr and call sbatch on it...
"""
function sbatch_julia_expr(expr)

    # placeholder ofr later
    slurm_args = Dict{String,Any}(                                                                             
    "job-name"=>"sbatch_test",
    "nodes"=>"1",
    "mem"=>"10GB", # test cause some crashed in memory and charles doin in cmmit #1267
    "time"=>"05:59:00",
    "mail-user"=>"jbenjami@caltech.edu",
    "mail-type"=>"ALL",
    "ntasks"=>"1",
    "gres"=>"gpu:1",
    "ntasks-per-node"=>"1",
    )

    bash_commands = [
        "echo running bash commands",
        "set -euo pipefail", # kill the job if anything fails
        # "set -x", # echo script line by line as it runs it
        "module purge",
        "module load julia/1.8.5", # rely on bashrc doesnt work, we gotta update this everytime we update julia...
        "module load hdf5/1.10.1",
        "module load netcdf-c/4.6.1",
        "module load cuda/11.2",
        "module load openmpi/4.0.4_cuda-10.2",
        "module load cmake/3.10.2", # CUDA-aware MPI
        #
        # "module load HDF5/1.8.19-mpich-3.2", # seems to be only sampo default
        # "module load netCDF/3.6.3-pgi-2017", # seems to be only sampo default
        # "module load openmpi/1.10.2/2017",  
        # "module load mpi/openmpi-x86_64",
        #
        raw"export JULIA_NUM_THREADS=${SLURM_CPUS_PER_TASK:=1}", # usin raw so we dont have to escape all the dollar signs and brackets
        # "export JULIA_DEPOT_PATH=$(CLIMA)/.julia_depot", # not sure why i commented this out, seems to lead to mpi binary has changed, please run pkg.build mpi...
        "export JULIA_MPI_BINARY=system", # does this cause a problem between nodes?
        "export JULIA_CUDA_USE_BINARYBUILDER=false",

    ]
    bash_commands_join = join(bash_commands,"; ")

    julia_precommands = []
    julia_precommands_join = join(julia_precommands,"; ")

    args = [
        "--eval '"*expr*"'"
    ]
    julia_command_options = join(args," ")

    # removed mpirun from here cause got error "mpirun does not support recursive calls" when trying to use this to call TC.jl runs w/ mpirun
    full_command = "--wrap=\""*bash_commands_join*":; julia --color yes $julia_command_options\"" # this the jawn we needed on god https://stackoverflow.com/a/33402070, wrap means sbatch wraps the string inside a sh call which helps with interpreting esp special characters and strings..., # backticks means a call to shell too
    println(full_command); println("")
    processed_slurm_args = ["--"*k*"="*string(v) for (k,v) in slurm_args] #get into a nice list of strings like slurm requires...

    job_ID = sbatch(full_command, processed_slurm_args)
    job_ID = parse(Int,split(job_ID)[end]) # get to Int


    return job_ID

end


"""
take a current julia expr and call sbatch on it...
"""
function sbatch_julia_expr_clima(expr)

    # placeholder ofr later
    slurm_args = Dict{String,Any}(                                                                             
    "job-name"=>"sbatch_test",
    "nodes"=>"1",
    "mem"=>"10GB", # test cause some crashed in memory and charles doin in cmmit #1267
    "time"=>"05:59:00",
    "mail-user"=>"jbenjami@caltech.edu",
    "mail-type"=>"ALL",
    "ntasks"=>"1",
    "gres"=>"gpu:1",
    "ntasks-per-node"=>"1",
    )

    bash_commands = [
        "echo running bash commands",
        "set -euo pipefail", # kill the job if anything fails
        # "set -x", # echo script line by line as it runs it
        "module purge",
        "module load julia/1.8.5", # rely on bashrc doesnt work, we gotta update this everytime we update julia...
        # "module load hdf5/1.10.1",
        # "module load netcdf-c/4.6.1",
        "module load cuda/julia_pref",
        "module load openmpi/4.1.5_cudax",
        # "module load cmake/3.10.2", # CUDA-aware MPI
        #
        # "module load HDF5/1.8.19-mpich-3.2", # seems to be only sampo default
        # "module load netCDF/3.6.3-pgi-2017", # seems to be only sampo default
        # "module load openmpi/1.10.2/2017",  
        # "module load mpi/openmpi-x86_64",
        #
        raw"export JULIA_NUM_THREADS=${SLURM_CPUS_PER_TASK:=1}", # usin raw so we dont have to escape all the dollar signs and brackets
        # "export JULIA_DEPOT_PATH=$(CLIMA)/.julia_depot", # not sure why i commented this out, seems to lead to mpi binary has changed, please run pkg.build mpi...
        "export JULIA_MPI_BINARY=system", # does this cause a problem between nodes?
        "export JULIA_CUDA_USE_BINARYBUILDER=false",

    ]
    bash_commands_join = join(bash_commands,"; ")

    julia_precommands = []
    julia_precommands_join = join(julia_precommands,"; ")

    args = [
        "--eval '"*expr*"'"
    ]
    julia_command_options = join(args," ")

    # removed mpirun from here cause got error "mpirun does not support recursive calls" when trying to use this to call TC.jl runs w/ mpirun
    full_command = "--wrap=\""*bash_commands_join*":; julia --color yes $julia_command_options\"" # this the jawn we needed on god https://stackoverflow.com/a/33402070, wrap means sbatch wraps the string inside a sh call which helps with interpreting esp special characters and strings..., # backticks means a call to shell too
    println(full_command); println("")
    processed_slurm_args = ["--"*k*"="*string(v) for (k,v) in slurm_args] #get into a nice list of strings like slurm requires...

    job_ID = sbatch(full_command, processed_slurm_args)
    job_ID = parse(Int,split(job_ID)[end]) # get to Int


    return job_ID

end

end # module
