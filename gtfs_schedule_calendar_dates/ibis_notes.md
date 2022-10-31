`ibis` - wrapper, translator between python and sql
`dask` - still using python, but splits to different computers to execute

`df.sum()`, pandas df, sum it, a single core on computer goes to python, 
`ddf.sum()`, sum it across partitioned dfs and sums it, and collects it into 1


`ibis` has a sql table somewhere (BQ is), and can access it with python interface, similar to siuba. can take ibis query, show me sql statement, and copy and paste that into BQ.

don't use `ibis` for regular jobs. it's simply to translate python to sql, develop sql code without writing sql. `ibis` can also write faster queries.