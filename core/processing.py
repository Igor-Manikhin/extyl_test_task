import asyncio

from core import settings
from core.models import TestModel


async def process_data_batch(batch_data: list[dict], semaphore: asyncio.Semaphore) -> None:
    print("--------- Processing data batch ----------")

    bulk_update_instances = []
    bulk_create_instances = []
    records_data = {item["id"]: item for item in batch_data}

    async with semaphore:
        existing_records = await TestModel.filter(id__in=records_data.keys()).all()
        existing_records_id_set = set(record.id for record in existing_records)
        batch_data_id_set = set(records_data.keys())

        non_existing_records_id = batch_data_id_set - existing_records_id_set

        for record in existing_records:
            if record_data := records_data.get(record.id):
                record.update_from_dict(record_data)
                bulk_update_instances.append(record)

        for record_id in non_existing_records_id:
            if create_record_data := records_data.get(record_id):
                bulk_create_instances.append(TestModel(**create_record_data))

        if bulk_create_instances:
            await TestModel.bulk_create(bulk_create_instances)

        if bulk_update_instances:
            await TestModel.bulk_update(
                bulk_update_instances,
                fields=["test_field_1", "test_field_2", "test_field_3", "test_field_4"],
            )

        print("--------- End of processing data batch ----------")


async def process_data(data: list[dict]) -> None:
    semaphore = asyncio.Semaphore(settings.CONCURRENTLY)

    create_records_tasks = [
        process_data_batch(
            data[batch_start: batch_start + settings.PROCESSING_BATCH_SIZE],
            semaphore,
        )
        for batch_start in range(0, len(data), settings.PROCESSING_BATCH_SIZE)
    ]

    await asyncio.gather(*create_records_tasks)
