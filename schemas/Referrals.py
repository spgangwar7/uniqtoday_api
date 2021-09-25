from pydantic import BaseModel

class ReferStudent(BaseModel):
    student_id:int
    exam_id:int
    email:str

class UpdateReferStudent(BaseModel):
    student_id:int
    referral_code:str
    email:str
    user_name:str
    phone:int

class SendReferralEmail(BaseModel):
    receiver_email:str
    sender_user_id:int
    link:str

