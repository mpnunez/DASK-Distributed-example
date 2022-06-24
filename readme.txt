# DASK distributed example

Template to customize for running DASK workers on a compute cluster.

Scheduler and Client are handled with python API. Worker
is launched from CLI in a separate python process.

Everything ramps up and down without nasty errors

- need click version 8.0.4, v8.1.0 is bad
- Need python3.7+ to use AsyncExitStack

