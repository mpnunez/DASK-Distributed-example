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

    def __aenter__(self):
        """
        Launch the worker
        """

        worker_exe = Path(sys.executable).parent / "dask-worker"

        worker_exe + " "

        #self.process = await asyncio.create_subprocess_exec(, shell=True)
        #print(f'Process pid is: {process.pid}')
        #status_code = await process.wait()
        #print(f'Status code: {status_code}')
        return self


    def __aexit__(self):
        pass

async def f():

    async with AsyncExitStack() as stack:
        
        # Start scheduler
        s = Scheduler(dashboard=True)
        await stack.enter_async_context(s)  # Scheduler must be running before client or workers connect
        print(s.address)
        
        # Start client
        client = Client(s.address, asynchronous=True)
        client_fut = stack.enter_async_context(client)

        
        # Start workers
        n_workers = 10
        #[await stack.enter_async_context(ClusterWorker(s.address)) for _i in range(n_workers)]

        await client_fut
        print(client.dashboard_link)
        future = client.submit(lambda x: x + 1, 10)
        result = await future
        print("Computation finished!")
        print(result)

        await client.shutdown()         # Shuts down all workers nicely

        




def main():
    asyncio.get_event_loop().run_until_complete(f())

if __name__ == "__main__":
    main()
