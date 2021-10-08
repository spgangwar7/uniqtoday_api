from typing import Optional,List

from pydantic.main import BaseModel

class answer_list(BaseModel):
    answer: float
    timetaken: str
    attemptCount: float
    question_id: float

class AdaptiveQuestionsMock(BaseModel):
    exam_id:int
    student_id:int
    subject_id :int
    chapter_id :int
    exam_type_id :int
    test_type_id :int

class AdaptiveQuestionsMock2(BaseModel):
    exam_id:int
    student_id:int

class AdaptiveQuestionsChapterPractice(BaseModel):
    exam_id:int
    student_id:int
    chapter_id :int
    session_id:int
    end_test:str
    questions_list:List[int]
    answerList:List[answer_list]

class AdaptiveQuestionsTopicPractice(BaseModel):
    exam_id:int
    student_id:int
    topic_id:int
    session_id:int
    end_test:str
    questions_list:List[int]
    answerList:List[answer_list]

