# Parallel programming with Julia

#Parallel programming is achieved by starting multiple processes and interacting with them using `RemoteCall`s and `RemoteRef`s.

#We start by checking the number of processes currently running.

nprocs()

#We can also check the number of cores we have

CPU_CORES

#And add processes to cover all cores

addedprocs=addprocs(CPU_CORES-1)

## Running code remotely

#We run code remotely using the `remotecall` function.  There's some good documentation about this on the [parallel-computing page](http://julia.readthedocs.org/en/latest/manual/parallel-computing/).  The examples below are from there.

r = remotecall(2, rand, 3, 4)

#`remotecall` executes a command (`rand` in this case) asynchronously on a different process (`2` in this case) and passes it a bunch of args (`3` and `4` in this case).

#The code is executed remotely and we receive a remote reference to it.  We can fetch the result using `fetch`.  `fetch` will wait for execution to complete before returning a result.  If the call does not return a result, but you need to wait for it to complete, you can also use the `wait` function.

fetch(r)

#You can also execute an expression in a remote process using the `@spawnmat` macro, which takes the process index as its first argument, and executes everything else remotely.

s = @spawnat 2 1 .+ fetch(r)

fetch(s)

#If you need the result of a remote execution immediately, you can call `remotecall_fetch()`.  This call will wait for the remote command to complete and then return its result.

remotecall_fetch(2, getindex, r, 1, 1)

#This particular call runs `getindex(r, 1, 1)` on the remote process, so only `r[1, 1]` is returned.  This is more efficient than fetching `r` and then inspecting `r[1, 1]`

## @spawn

#The `@spawn` macro is a more convenient way to run code on a remote process, and is also smart enough to decide based on locality of references, when to fetch data locally and when to send code to the remote process for execution.

r = @spawn rand(3, 4)

s = @spawn 1 .+ fetch(r)

fetch(s)

## @parallel

#You can also parallelise a `for x in range` style loop using the `@parallel` macro.  This macro partitions the range and runs each subrange on a different process. It can intelligently determine which process to send the next iteration to.

#The result of each iteration (ie, the value of the last statement executed) may optionally be passed to a `reducer` function whose job is to reduce multiple results to a single result, operating on two at a time (ie, similar to the `reduce` function or `mapreduce`).

nheads = @parallel (+) for i in 1:200000
    rand(0:1)
end

#In the example above, we call the `rand` function 200000 times, and then add the results together.  The `@parallel` macro decides how to partition the 200000 iterations, across all workers.  It then calls the `+` function on the results, two at a time.  The `+` function is called on each process separately to reduce each batch, and then finally called using the results of each batch to produce the final result.  To make sure this works consistently, the reducer function should be #**Associative**.

#Note that the parallel macro only works on range loops, or on numeric vectors.

# Add all numbers from 1-5 and 10-15
a=[1:5, 10:15]
nheads = @parallel (+) for i in a
    i
end

### Other iterable types
#To operate on other iterable types, you need to create a range of indexes, and then use an index lookup within the loop.  For example, if we have a `Dict`, we need to first create an array of keys for this `Dict`, and then use the indexes of this key array as our range:

# Create the Dict
a=Dict(["a" => 1, "b" => 2, "c" => 3])

# Create an array of keys
ks = collect(keys(a))

# Iterate through the range of the key index
nheads = @parallel (+) for i in 1:length(ks)
    # Lookup the key.  The ks Array is copied to all processes
    k = ks[i]

    # Finally lookup the value.  The a Dict is copied to all processes
    v = a[k]
end

### Defining our own reducer
#Note that in the above examples, we've included the reducer function `+` inside parentheses.  This is only required to avoid ambiguity since the `+` function is usable as an infix operator.  For functions that cannot be used as infix operators, the parentheses are not required.

# Create the Dict
a=Dict(["a" => 1, "b" => 2, "c" => 3])

# Create an array of keys
ks = collect(keys(a))

# Define a reducer function to add
function add2numbers(a, b)
    return a+b
end

# Iterate through the range of the key index
nheads = @parallel add2numbers for i in 1:length(ks)
    # Lookup the key.  The ks Array is copied to all processes
    k = ks[i]

    # Finally lookup the value.  The a Dict is copied to all processes
    v = a[k]
end

### Returning multiple values from a loop
#Your loop can only return a single value, and the reducer function will only accept two arguments of the same type as the loop's return value, however, using Julia's `tuple` type, you can easily return a `tuple` of multiple values and have your reducer operate on them.

#In the following example, we operate on the key and the value of the `Dict` we've been using above

# Create the Dict
a=Dict(["a" => 1, "b" => 2, "c" => 3])

# Create an array of keys
ks = collect(keys(a))

function reducer(x, y)
    k = x[1] * y[1]
    v = x[2] + y[2]

    (k, v)
end

# Iterate through the range of the key index
nheads = @parallel reducer for i in 1:length(ks)
    # Lookup the key.  The ks Array is copied to all processes
    k = ks[i]

    # Finally lookup the value.  The a Dict is copied to all processes
    v = a[k]

    (k, v)
end

## Cleaning up

#After we're done, it may be a good idea to remove any workers that we've created so they don't hang around idle.  For this tutorial, it's a good idea so that we start with a clean slat

addedprocs

rmprocs(addedprocs)

nprocs()

## Further reading

#This has been a very brief introduction to parallel computing in Julia.  There is much more you can accomplish using clusters, multiple hosts, Shared and Distributed Data Structures and Synchronization.  Read the [Julia docs on Parallel Computing](http://julia.readthedocs.org/en/latest/manual/parallel-computing/) and the [Manual for Tasks and Parallel Computing](http://julia.readthedocs.org/en/latest/stdlib/parallel/) for more details.

#Also check out the [JuliaParallel repository on GitHub](https://github.com/JuliaParallel) for many packages that make Parallel Computing easier.
