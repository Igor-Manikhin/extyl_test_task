from tortoise import Tortoise

from core import settings


async def init() -> None:
    await Tortoise.init(
        config=settings.TORTOISE_ORM,
    )
    await Tortoise.generate_schemas()
