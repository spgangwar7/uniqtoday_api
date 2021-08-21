from typing import Optional,List

from pydantic.main import BaseModel

class questionPrediction(BaseModel):
    question_id:List[int]=None
    attempt_status:List[str]=None
    unique_question_list:str=None
    class_exam_id:int

class adaptiveQuestions(BaseModel):
    exam_id:int
    student_id:int
    total_questions:int



