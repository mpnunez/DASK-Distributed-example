#! ./.venv/bin/python

"""
Modified from
https://docs.dask.org/en/latest/deploying-python-advanced.html
"""

import asyncio
from dask.distributed import Scheduler, Worker, Client
import sys
from pathlib import Path
import subprocess

WORKER_CONNECT_WAIT = 3     # seconds after worker process starts to connect to scheduler


async def f():
    async with Scheduler(scheduler_file="scheduler_info.json") as s:
        async with Client(s.address, asynchronous=True) as client:

            # Start worker in separate python process
            worker_exe = Path(sys.executable).parent / "dask-worker"
            worker_cmd = [f"{worker_exe} {s.address}"]
            

            future = client.submit(lambda x: x + 1, 10)
            result = await future

            """
            #worker_process = subprocess.Popen(worker_cmd,shell=True)
            await asyncio.sleep(3)
            print(s.workers)

            await asyncio.sleep(3)
            print(s.workers)

            workers = list(s.workers)

            
            print("Computation finished!")
            print(result)

            # Get workers and retire them
            print(s.workers)
            workers = list(s.workers)
            print("WORKERS")
            print(workers)
            await s.retire_workers(s.workers.keys(), close_workers=True)
            #await asyncio.sleep(3)
            
            #worker_process.kill()
            print("SHOULD EXIT NOW")
            """

asyncio.get_event_loop().run_until_complete(f())



def main():
    asyncio.get_event_loop().run_until_complete(f())

if __name__ == "__main__":
    main()
