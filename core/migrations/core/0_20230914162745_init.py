from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE IF NOT EXISTS "testmodel" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "test_field_1" VARCHAR(255) NOT NULL,
    "test_field_2" VARCHAR(255) NOT NULL,
    "test_field_3" VARCHAR(255) NOT NULL,
    "test_field_4" VARCHAR(255) NOT NULL,
    "is_deleted" BOOL NOT NULL  DEFAULT False
);
COMMENT ON COLUMN "testmodel"."id" IS 'Первичный ключ';
COMMENT ON COLUMN "testmodel"."test_field_1" IS 'Строковое поле 1';
COMMENT ON COLUMN "testmodel"."test_field_2" IS 'Строковое поле 2';
COMMENT ON COLUMN "testmodel"."test_field_3" IS 'Строковое поле 3';
COMMENT ON COLUMN "testmodel"."test_field_4" IS 'Строковое поле 4';
COMMENT ON TABLE "testmodel" IS 'Тестовая модель';
CREATE TABLE IF NOT EXISTS "aerich" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "version" VARCHAR(255) NOT NULL,
    "app" VARCHAR(100) NOT NULL,
    "content" JSONB NOT NULL
);"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        """
