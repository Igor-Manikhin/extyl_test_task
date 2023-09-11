from tortoise import Tortoise

from core import settings


def get_db_url() -> str:
    return (
        f"asyncpg://{settings.DB_USERNAME}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/"
        f"{settings.DB_NAME}"
    )


async def init() -> None:
    await Tortoise.init(
        db_url=get_db_url(),
        modules={"models": ["core.models"]}
    )
    await Tortoise.generate_schemas()
