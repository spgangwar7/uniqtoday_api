from tortoise import fields, models
from tortoise.contrib.pydantic import pydantic_model_creator
from tortoise.models import Model


class LeadershipBoard(Model):

    id = fields.IntField(pk=True)
    #: This is a username
    user_id = fields.IntField(null=True)
    month = fields.IntField(null=True)
    year = fields.IntField(null=True)
    publish_date = fields.DateField(null=True)
    class_exam_id = fields.IntField(null=True)
    subject_id = fields.IntField(null=True)
    marks = fields.IntField(null=True)
    test_series_id = fields.IntField(null=True)
    updated_at = fields.DatetimeField(auto_now_add=True)
    created_at = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "leadership_board"

LeadershipBoard_Pydantic = pydantic_model_creator(LeadershipBoard, name="LeadershipBoard")
LeadershipBoardIn_Pydantic = pydantic_model_creator(LeadershipBoard, name="LeadershipBoardIn", exclude_readonly=True)

