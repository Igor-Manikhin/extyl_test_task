import asyncio

from tortoise import run_async

from core import settings
from core.init_db import init
from core.processing import process_data
from core.settings import SLEEP_TIME_SECONDS
from core.utils import generate_json_list


async def main():
    await init()

    while True:
        print("Start processing json data list")

        json_list = generate_json_list(list_size=settings.LIST_DATA_COUNT)
        await process_data(json_list)
        print(f"Processing json data list is completed. Sleep on {SLEEP_TIME_SECONDS} seconds")
        await asyncio.sleep(SLEEP_TIME_SECONDS)

run_async(main())
