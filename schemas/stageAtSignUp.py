from pydantic import BaseModel


class stageAtSignUp(BaseModel):
    student_id:int
    student_stage_at_sgnup:int