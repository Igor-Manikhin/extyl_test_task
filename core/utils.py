import random
import string


def create_random_string(length: int = 30) -> str:
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(length))


def generate_json_list(list_size: int = 100) -> list[dict]:
    random_ids = random.sample(range(1, list_size * 2), list_size)

    return [
        {
            "id": random_id,
            "test_field_1": create_random_string(),
            "test_field_2": create_random_string(),
            "test_field_3": create_random_string(),
            "test_field_4": create_random_string(),
        }
        for random_id in random_ids
    ]
