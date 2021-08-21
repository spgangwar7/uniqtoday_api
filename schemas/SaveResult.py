from pydantic import BaseModel
from typing import List

class answer_list(BaseModel):
    answer: float
    timetaken: str
    attemptCount: float
    question_id: float

class SaveResult(BaseModel):
    user_id:str
    test_time:str
    time_taken:str
    class_id:int
    questions_list:List[int]
    total_marks:float
    no_of_question:float
    answerList:List[answer_list]

