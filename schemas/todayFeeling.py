from pydantic import BaseModel
class todayFeeling(BaseModel):
    user_id:int
    user_mood_ind:int