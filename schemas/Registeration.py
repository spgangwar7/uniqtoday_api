from typing import Optional
from pydantic.main import BaseModel

class Register(BaseModel):
    email:str
    mobile:int

class StudentSignup(BaseModel):
    user_name:str
    email:str
    mobile:int

class StudentLogin(BaseModel):
    mobile_otp:int
    mobile:int
