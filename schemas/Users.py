from typing import Optional

from pydantic.main import BaseModel


class Users(BaseModel):
    stream_code:int
    address:str
    city:str
    country:str
    email:str
    first_name :str
    gender:str
    grade_id:int
    institution_id:int
    last_name:str
    user_name:str
    mobile:int
    password:str
    state:str
    status :str
    zipcode:int

class UpdateUsers(BaseModel):
    id:int
    email:str
    first_name :str
    last_name:str
    user_name:str
    mobile:int
