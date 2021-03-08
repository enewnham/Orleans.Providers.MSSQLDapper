# Orleans.Providers.MSSQLDapper
Optimized Orleans providers for Microsoft SQL Server for In-Memory OLTP and Natively Compiled Stored Procedures

### Benchmarks

I ran the benchmarks by using the official Orleans repository's benchmark tool. I had to hack it together because I don't have the bandwidth to write a proper one for this experiment

```
> dotnet run -c Release -f net5.0 -- GrainStorage.AdoNet
Running grain storage benchmark against AdoNet
Performed 233073 persist (read & write) operations with 0 failures in 33318ms.
Average time in ms per call was 13.944696902686891, with longest call taking 7818.9575ms.
Total time waiting for the persistent store was 3250132.341199942ms.
Elapsed milliseconds: 33348
Press any key to continue ...

> dotnet run -c Release -f net5.0 -- GrainStorage.MSSQLDapper
Running grain storage benchmark against MSSQL + Dapper
Performed 393911 persist (read & write) operations with 0 failures in 30760ms.
Average time in ms per call was 7.519319598843245, with longest call taking 2307.6125ms.
Total time waiting for the persistent store was 2961942.7024999415ms.
Elapsed milliseconds: 30825
Press any key to continue ...
```
