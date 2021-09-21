from pydantic import BaseModel


class UpdateToken (BaseModel):
    token:str
    user_id:int

class TestNotification (BaseModel):
    token:str
    heading:str
    body:str