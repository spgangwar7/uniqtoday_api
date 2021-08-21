from pydantic import BaseModel

class ReferStudent(BaseModel):
    student_id:int
    exam_id:int
    email:str
