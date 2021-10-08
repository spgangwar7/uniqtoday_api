from pydantic import BaseModel
from typing import Optional

class StudentPlanners(BaseModel):
    student_id: int
    exam_id: int
    subject_id:int
    chapter_id: str
    #planned_test_week_days: str
    date_from: str
    date_to: str
