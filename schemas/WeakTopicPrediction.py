from typing import Optional,List

from pydantic.main import BaseModel

class WeakTopic(BaseModel):
    subject:str
    topics:dict
