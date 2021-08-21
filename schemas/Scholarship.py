from pydantic import BaseModel

class ScholarshipProfilingQuestion(BaseModel):
    student_id:int
    question_cnt:int
    exam_id:int