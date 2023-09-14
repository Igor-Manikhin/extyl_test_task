import asyncio

from core import settings
from core.models import TestModel


class APIDataProcessor:
    def __init__(self, max_rate: int, concurrent_level: int = None):
        self.__max_rate = max_rate
        self._concurrent_level = concurrent_level
        self.__create_queue = asyncio.Queue()
        self.__update_queue = asyncio.Queue()
        self.__semaphore = asyncio.Semaphore(concurrent_level or max_rate)
        self.__scheduler_task = None

    async def __produce_data_batch(self, batch_data: list[dict]):
        bulk_update_instances = []
        bulk_create_instances = []
        records_data = {item["id"]: item for item in batch_data}

        existing_records = await TestModel.filter(id__in=records_data).all()
        existing_records_id_set = set(record.id for record in existing_records)
        batch_data_id_set = set(records_data)

        non_existing_records_id = batch_data_id_set - existing_records_id_set

        for record in existing_records:
            if record_data := records_data.get(record.id):
                record.update_from_dict(record_data)
                bulk_update_instances.append(record)

        for record_id in non_existing_records_id:
            if create_record_data := records_data.get(record_id):
                bulk_create_instances.append(TestModel(**create_record_data))

        if bulk_create_instances:
            await self.__create_queue.put(bulk_create_instances)

        if bulk_update_instances:
            await self.__update_queue.put(bulk_update_instances)

    async def produce_data(self, data: list[dict]):
        processing_data_tasks = [
            self.__produce_data_batch(
                data[batch_start: batch_start + settings.PROCESSING_BATCH_SIZE],
            )
            for batch_start in range(0, len(data), settings.PROCESSING_BATCH_SIZE)
        ]

        await asyncio.gather(*processing_data_tasks)

    async def update_worker(self):
        while True:
            bulk_update_instances = await self.__update_queue.get()

            await TestModel.bulk_update(
                bulk_update_instances,
                fields=["test_field_1", "test_field_2", "test_field_3", "test_field_4"],
                batch_size=settings.UPDATE_RECORDS_BATCH_SIZE,
            )
            print("Updated")

            self.__update_queue.task_done()

    async def __create_worker(self, worker_number: int):
        bulk_create_instances = await self.__create_queue.get()

        async with self.__semaphore:
            print(f"Worker {worker_number} len batch: {len(bulk_create_instances)}")

            await TestModel.bulk_create(
                bulk_create_instances,
                batch_size=settings.CREATE_RECORDS_BATCH_SIZE,
            )
            print(f"Worker {worker_number}: Created")

            self.__create_queue.task_done()

    async def __scheduler(self):
        asyncio.create_task(self.update_worker())

        while True:
            tasks = []

            for i in range(self.__max_rate):
                tasks.append(self.__create_worker(i))

            await asyncio.gather(*tasks)

    async def join(self):
        await self.__create_queue.join()
        print("Create queue joined!")
        await self.__update_queue.join()
        print("Update queue joined!")

    def start(self):
        self.__scheduler_task = asyncio.create_task(self.__scheduler())
