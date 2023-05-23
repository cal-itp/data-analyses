# Dask Notes

* Saving out partitioned parquets and reading it back in can break up some of the memory issues. Even with `delayed` objects, needing to `compute` and write it as a single parquet can kill the kernel. Just write out as partitioned parquet (directory of parquet files) and read it in and concat it.
* `map_partitions` seems to error when running in the cloud, if there are too many steps before. Some of the delayed dfs calculated don't make its way through? `persist`.
* What's the difference between: 
    * 
    ```# this works when ddf.compute() is called
    results2 = [compute(i)[0] for i in results]
    ddf = dd.multi.concat(results2, axis=0).reset_index(drop=True)
    ```
    * `dd.compute(*results)` [https://docs.dask.org/en/stable/delayed-collections.html](docs) - this doesn't work when the pandas df that results needs `df.compute()`.
    * How to better manipulate `delayed` objects through the workflow to actually do the compute once?
    

## Client
* [GH: Dask workers don't reload modified modules](https://github.com/dask/distributed/issues/4245)