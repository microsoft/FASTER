# Sum Store

The sum store example allows you to test FASTER operations as well
as recovery. It has two modes: `concurrency_test` and `recovery_test`.

## Concurrency Test

The `concurrency_test` runs a multi-threaded sequence of RMW operations
on FASTER, and tests the final result for correctness. Run it as
follows:

```
SumStore.exe concurrency_test 2
```

Here, you can replace `2` by the number of threads to feed FASTER using.

## Recovery Test

The `recovery_test` runs a sequence of RMW operations, with periodic
checkpointing:

```
SumStore.exe recovery_test 2 populate
```

At any time, one may kill the execution, and recover and continue the test 
as follows:

```
SumStore.exe recovery_test 2 continue
```

Further, one may recover to either the latest or a given checkpoint ID, and
test that the contents of the database are consistent with the CPR sequence
numbers reported by FASTER to client threads.

Recover to latest checkpoint:
```
SumStore.exe recovery_test 2 recover
```

Recover to specific checkpoint:
```
SumStore.exe recovery_test 2 recover SINGLE_GUID 
SumStore.exe recovery_test 2 recover INDEX_GUID LOG_GUID 
```
