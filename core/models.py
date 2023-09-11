from tortoise import fields
from tortoise import models


class TestModel(models.Model):
    """Тестовая модель"""

    id = fields.IntField(pk=True, description="Первичный ключ")
    test_field_1 = fields.CharField(max_length=255, description="Строковое поле 1")
    test_field_2 = fields.CharField(max_length=255, description="Строковое поле 2")
    test_field_3 = fields.CharField(max_length=255, description="Строковое поле 3")
    test_field_4 = fields.CharField(max_length=255, description="Строковое поле 4")
    is_deleted = fields.BooleanField(default=False)

    def __str__(self):
        return self.id
