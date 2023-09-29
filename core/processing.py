import asyncio

from tortoise.transactions import in_transaction

from core import settings
from core.models import TestModel


class APIDataProcessor:
    def __init__(self, max_rate: int, concurrent_level: int = None):
        self.__max_rate = max_rate
        self._concurrent_level = concurrent_level
        self.__existing_records_id_set = set()
        self.__create_queue = asyncio.Queue()
        self.__update_queue = asyncio.Queue()
        self.__semaphore = asyncio.Semaphore(concurrent_level or max_rate)
        self.__create_scheduler_task = None
        self.__update_scheduler_task = None
        self.__event = asyncio.Event()

    async def __produce_data_batch(self, batch_data: list[dict]):
        bulk_update_instances = []
        bulk_create_instances = []
        records_data = {item["id"]: item for item in batch_data}

        existing_records = await TestModel.filter(id__in=records_data).all()
        existing_records_id_set = set(record.id for record in existing_records)
        batch_data_id_set = set(records_data)

        non_existing_records_id = batch_data_id_set - existing_records_id_set
        self.__existing_records_id_set.update(existing_records_id_set)

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

    async def __recreate_table(self) -> None:
        print("Recreating table!")

        async with in_transaction() as conn:
            await conn.execute_query(
                f"CREATE TABLE testmodel2 AS SELECT testmodel.* FROM testmodel LEFT JOIN"
                f"(VALUES {','.join(f'({i})' for i in self.__existing_records_id_set)}) temp(temp_id)"
                f"ON testmodel.id = temp.temp_id WHERE temp.temp_id IS NULL"
            )
            await conn.execute_query("DROP TABLE testmodel")
            await conn.execute_query("ALTER TABLE testmodel2 RENAME TO testmodel")

        print("Recreating completed!")

    async def __update_scheduler(self):
        while True:
            await self.__event.wait()

            if self.__update_queue.qsize():
                await self.__recreate_table()

            while self.__update_queue.qsize():
                tasks = [
                    asyncio.create_task(self.__worker(self.__update_queue, i))
                    for i in range(self.__max_rate)
                ]

                await asyncio.gather(*tasks)

            self.__existing_records_id_set.clear()
            self.__event.clear()

    async def __worker(self, queue: asyncio.Queue, worker_number: int):
        bulk_create_instances = await queue.get()

        async with self.__semaphore:
            print(f"Worker {worker_number} len batch: {len(bulk_create_instances)}")

            await TestModel.bulk_create(
                bulk_create_instances,
                batch_size=settings.CREATE_RECORDS_BATCH_SIZE,
            )
            print(f"Worker {worker_number}: Created")

            queue.task_done()

    async def __create_scheduler(self):
        while True:
            tasks = [
                asyncio.create_task(self.__worker(self.__create_queue, i))
                for i in range(self.__max_rate)
            ]

            await asyncio.gather(*tasks)

    async def join(self):
        await self.__create_queue.join()
        print("Create queue joined!")
        self.__event.set()
        await self.__update_queue.join()
        print("Update queue joined!")

    def start(self):
        self.__create_scheduler_task = asyncio.create_task(self.__create_scheduler())
        self.__update_scheduler_task = asyncio.create_task(self.__update_scheduler())
