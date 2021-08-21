from tortoise import fields, models
from tortoise.contrib.pydantic import pydantic_model_creator
from enum import Enum, IntEnum
from tortoise.models import Model

class StudentPlanners(models.Model):
    id = fields.IntField(pk=True)
    student_id = fields.IntField(null=True)
    exam_id = fields.IntField(null=True)
    subject_id = fields.IntField(null=True)
    topic_id = fields.IntField(null=True)
    question_count = fields.IntField(null=True)
    test_time_in_min = fields.IntField(null=True)
    actual_ques_count = fields.IntField(null=True)
    created_on = fields.DatetimeField(auto_now=True)
    chapter_id = fields.IntField(null=True)
    actual_ques_count = fields.IntField(null=True)
    planned_test_week_days=fields.JSONField(null=True)
    date_from = fields.DateField(null=True)
    date_to = fields.DateField(null=True)

    class Meta:
        table = "student_planner"


StudentPlanner_Pydantic = pydantic_model_creator(StudentPlanners, name="StudentPlanner")
StudentPlannerIn_Pydantic = pydantic_model_creator(StudentPlanners, name="StudentPlannerIn", exclude_readonly=True)