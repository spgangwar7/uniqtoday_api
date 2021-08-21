from tortoise import fields
from tortoise.contrib.pydantic import pydantic_model_creator
from enum import Enum, IntEnum
from tortoise.models import Model

class prevYearQuestPaper(Model):
    id=fields.IntField(pk=True)
    exam_id=fields.IntField(null=False)
    subject_id=fields.IntField(null=False)
    exam_year=fields.IntField(null=False)
    paper_file_name=fields.CharField(max_length=50,null=True)
    updated_on = fields.DatetimeField(auto_now_add=True)
    created_on = fields.DatetimeField(auto_now=True)

    class Meta:
        table = "exam_rev_year_ques_papers"

prevYearQuestPaper_Pydantic = pydantic_model_creator(prevYearQuestPaper, name="question-paper-out")
prevYearQuestPaperIn_Pydantic = pydantic_model_creator(prevYearQuestPaper, name="question-paper-In", exclude_readonly=True)