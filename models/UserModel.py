from tortoise import fields, models
from tortoise.contrib.pydantic import pydantic_model_creator
from enum import Enum, IntEnum
from tortoise.models import Model


class User(Model):
    class Gender(str, Enum):
        Male = "Male"
        Female = "Female"

    class StreamCode(str, Enum):
        S = "S"
        H = "H"
        O = "O"
        C = "C"

    class Status(str, Enum):
        zero = '0'
        one = '1'

    id = fields.IntField(pk=True)
    #: This is a username
    first_name = fields.CharField(max_length=50, null=False)
    user_name = fields.CharField(max_length=30, null=True)
    student_sys_id = fields.CharField(max_length=15, null=True)
    last_name = fields.CharField(max_length=191, null=True)
    user_profile_img = fields.CharField(max_length=400, null=True)
    email = fields.CharField(max_length=191, unique=True, null=False)
    mobile = fields.BigIntField(unique=True, null=False)
    password = fields.CharField(max_length=100, null=True)
    auth_code = fields.CharField(max_length=50, null=True)
    address = fields.CharField(max_length=200, null=True)
    city = fields.CharField(max_length=40, null=True)
    state = fields.CharField(max_length=4, null=True)
    zipcode = fields.CharField(max_length=7, null=True)
    gender:Gender = fields.CharEnumField(Gender,null=True )
    country = fields.IntField(null=True)
    institution_id = fields.IntField(null=True)
    status: Status = fields.CharEnumField(Status, default=Status.zero)
    grade_id = fields.IntField(null=True)
    stream_code:StreamCode = fields.CharEnumField(StreamCode,default=StreamCode.O)
    updated_at = fields.DatetimeField(auto_now_add=True)
    created_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "student_users"

User_Pydantic = pydantic_model_creator(User, name="User")
UserIn_Pydantic = pydantic_model_creator(User, name="UserIn", exclude_readonly=True)

