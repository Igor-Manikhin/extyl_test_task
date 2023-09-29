import time

from tortoise import run_async

from core import settings
from core.init_db import init
from core.processing import APIDataProcessor
from core.settings import SLEEP_TIME_SECONDS
from core.utils import generate_json_list


async def main():
    offset = 0
    await init()

    data_processor = APIDataProcessor(max_rate=10, concurrent_level=5)
    data_processor.start()

    while True:
        print("Start processing json data list")
        start_time = time.perf_counter()

        json_list = generate_json_list(list_size=settings.LIST_DATA_COUNT, offset=offset)

        await data_processor.produce_data(json_list)
        await data_processor.join()

        print(f"Processing json data list is completed. Elapsed time: {time.perf_counter()  - start_time} seconds")
        print(f"---------- Sleep on {SLEEP_TIME_SECONDS} seconds ----------")

        offset += settings.LIST_DATA_COUNT // 2

run_async(main())
