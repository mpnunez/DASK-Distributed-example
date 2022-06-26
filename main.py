#! ./.venv/bin/python

"""
https://docs.dask.org/en/latest/deploying-python-advanced.html
"""

import asyncio
from dask.distributed import Scheduler, Worker, Client

from contextlib import AsyncExitStack

import sys
from pathlib import Path

class ClusterWorker:
    """
    Launches a worker on another machine

    Need to customize this class for your HPC cluster
    """

    def __init__(self,scheduler_address=None):
        self.process = None
        self.scheduler_address = scheduler_address

    async def __aenter__(self):
        """
        Launch the worker
        """

        worker_exe = Path(sys.executable).parent / "dask-worker"

        #worker_cmd = [f"{worker_exe}", "--no-reconnect", f"{self.scheduler_address}"]
        worker_cmd = [f"{worker_exe}", f"{self.scheduler_address}"]
        #worker_cmd = [f"{worker_exe}", "--no-reconnect", "--death-timeout '30'", f"{self.scheduler_address}"]
        print(worker_cmd)
        self.process = await asyncio.create_subprocess_exec(*worker_cmd)
        print("RETURN CODE: "+ str(self.process.returncode))
        return self.process


    async def __aexit__(self, exc_type, exc, tb):
        print("EXIT CONTEXT FOR WORKER")
        if self.process.returncode is None:
            pass
            print("KILLING SUBPROCESS")
            self.process.kill()

async def f():

    async with AsyncExitStack() as stack:
        
        # Start scheduler
        s = Scheduler(dashboard=True)
        await stack.enter_async_context(s)  # Scheduler must be running before client or workers connect
        print(s.address)
        
        #s.address = "tcp://10.0.0.167:8786"

        # Start client
        client = Client(s.address, asynchronous=True)
        #client_fut = stack.enter_async_context(client)

        
        # Start workers
        n_workers = 5
        child_procs = [await stack.enter_async_context(ClusterWorker(s.address)) for _i in range(n_workers)]
        #child_procs = [await stack.enter_async_context(Worker(s.address)) for _i in range(n_workers)]

        #await client_fut
        future = client.submit(lambda x: x + 1, 10)
        result = await future
        print("Computation finished!")
        print(result)

        """
        workers = client.scheduler_info()['workers']
        print("These are the workers")
        print(workers)
        print(type(workers))

        print(client.dashboard_link)
        
        await asyncio.sleep(500)
        result = await future
        print("Computation finished!")
        



        #workers = list(client.scheduler_info()['workers'])
        #print(workers)
        #await client.run_on_scheduler(lambda dask_scheduler=None: 
        #    dask_scheduler.retire_workers(workers, close_workers=True))

        """

        # Shuts down worker processes
        
        print("client.shutdown")
        #await client.shutdown()
        wmi = await s.get_worker_monitor_info()
        print(wmi)

        wtc = s.workers_to_close(n=n_workers,minimum=0)
        print(wtc)
        #for w in wtc:
        #    await s.retire_workers(w)
        #await s.retire_workers(s.workers)
        #await s.close_workers()
        #await client.close()
        

        print("Wait for subprocesses to end")
        #[await cp.communicate() for cp in child_procs]
        print("subprocesses should be done")

        

        




def main():
    asyncio.get_event_loop().run_until_complete(f())

if __name__ == "__main__":
    main()
