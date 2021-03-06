from pydantic import BaseModel
from typing import List

class answer_list(BaseModel):
    answer: float
    timetaken: str
    attemptCount: float
    question_id: float

class SaveResult(BaseModel):
    user_id:int
    test_time:str
    time_taken:str
    class_id:int
    questions_list:List[int]
    total_marks:float
    no_of_question:int
    answerList:List[answer_list]
    test_type:str
    exam_mode:str
    exam_type:str
    planner_id:int
    live_exam_id:int

