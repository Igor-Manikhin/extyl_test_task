# DB settings
TORTOISE_ORM = {
    "connections": {
        "default":  {
            "engine": "tortoise.backends.asyncpg",
            "credentials": {
                "host": "localhost",
                "port": "5432",
                "user": "admin",
                "password": "admin",
                "database": "extyl_test_task_db",
            }
        },
    },
    "apps": {
        "core": {
            "models": ["core.models", "aerich.models"],
            "default_connection": "default",
        },
    },
}

# Data processing settings
SLEEP_TIME_SECONDS = 5
LIST_DATA_COUNT = 150000
PROCESSING_BATCH_SIZE = 5000
UPDATE_RECORDS_BATCH_SIZE = 200
CREATE_RECORDS_BATCH_SIZE = 200
