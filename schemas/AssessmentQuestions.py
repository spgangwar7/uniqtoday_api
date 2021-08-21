from pydantic import BaseModel
class AssessmentQuestions(BaseModel):
    student_id:int
    exam_id:int
    count:int