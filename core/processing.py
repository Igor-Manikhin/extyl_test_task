import asyncio

from core import settings
from core.models import TestModel


async def process_data_batch(batch_data: list[dict], semaphore: asyncio.Semaphore) -> None:
    bulk_update_instances = []
    bulk_create_instances = []

    async with semaphore:
        existing_records = await TestModel.filter(id__in=[
            batch_data_item["id"] for batch_data_item in batch_data
        ]).all()
        existing_records_id_set = set(record.id for record in existing_records)
        batch_data_id_set = set(batch_data_item["id"] for batch_data_item in batch_data)

        non_existing_records_id = batch_data_id_set - existing_records_id_set

        for record in existing_records:
            record_update_data = list(filter(lambda item: item["id"] == record.id, batch_data))

            if record_update_data:
                record_update_data = record_update_data[0]
                record.test_field_1 = record_update_data["test_field_1"]
                record.test_field_2 = record_update_data["test_field_2"]
                record.test_field_3 = record_update_data["test_field_3"]
                record.test_field_4 = record_update_data["test_field_4"]
                bulk_update_instances.append(record)

        for record_id in non_existing_records_id:
            create_record_data = list(filter(lambda item: item["id"] == record_id, batch_data))

            if create_record_data:
                create_record_data = create_record_data[0]
                bulk_create_instances.append(
                    TestModel(
                        id=create_record_data["id"],
                        test_field_1=create_record_data["test_field_1"],
                        test_field_2=create_record_data["test_field_2"],
                        test_field_3=create_record_data["test_field_3"],
                        test_field_4=create_record_data["test_field_4"],
                    )
                )

        if bulk_create_instances:
            await TestModel.bulk_create(bulk_create_instances)

        if bulk_update_instances:
            await TestModel.bulk_update(
                bulk_update_instances,
                fields=["test_field_1", "test_field_2", "test_field_3", "test_field_4"],
            )


async def process_data(data: list[dict]) -> None:
    semaphore = asyncio.Semaphore(settings.PROCESSING_BATCH_SIZE)

    create_records_tasks = [
        process_data_batch(
            data[batch_start: batch_start + settings.PROCESSING_BATCH_SIZE],
            semaphore,
        )
        for batch_start in range(0, len(data), settings.PROCESSING_BATCH_SIZE)
    ]

    await asyncio.gather(*create_records_tasks)
