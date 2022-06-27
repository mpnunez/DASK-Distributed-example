#! ./.venv/bin/python

"""
https://docs.dask.org/en/latest/deploying-python-advanced.html
"""

import asyncio
from dask.distributed import Scheduler, Worker, Client

from contextlib import AsyncExitStack

import sys
from pathlib import Path

import subprocess

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
        #self.process = await asyncio.create_subprocess_exec(*worker_cmd)
        self.process = subprocess.Popen(f"{worker_exe} {self.scheduler_address}",shell=True)
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

        # Start client
        client = Client(s.address, asynchronous=True)
        await stack.enter_async_context(client)

        async with AsyncExitStack() as worker_stack:

            # Start workers
            n_workers = 5
            child_procs = [await worker_stack.enter_async_context(ClusterWorker(s.address)) for _i in range(n_workers)]
            #child_procs = [await stack.enter_async_context(Worker(s.address)) for _i in range(n_workers)]

            #await client_fut
            future = client.submit(lambda x: x + 1, 10)
            result = await future
            print("Computation finished!")
            print(result)



def main():
    asyncio.get_event_loop().run_until_complete(f())

if __name__ == "__main__":
    main()
