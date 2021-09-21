from pydantic import BaseModel

class ReferStudent(BaseModel):
    student_id:int
    exam_id:int
    email:str

class UpdateReferStudent(BaseModel):
    student_id:int
    exam_id:int
    email:str
    user_name:str
    phone:int

class sendReferralEmail(BaseModel):
    receiver_email:str
    sender_user_id:int
    link:str

