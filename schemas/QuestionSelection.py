from pydantic.main import BaseModel

class Question_Selection(BaseModel):
    student_id_input:int
    exam_id:int
    no_incorrect_repeat_questions:int
    total_time:int

class Advance_Question_Selection(BaseModel):
    test_type:str
    student_id:int
    exam_id:int
    question_cnt:int
    difficulty_level:int
    question_from:str
    question_category:str
    subject_list:str
    topic_list:str

class AdvanceQuestionSelectiontest(BaseModel):
    test_type: str
    student_id: int
    exam_id: int
    question_cnt: int
    difficulty_level: int
    question_from: str
    question_category: str
    subject_list: str
    topic_list: str

class AdvanceQuestionSelectiontest2(BaseModel):
    student_id: int
    exam_id: int
    question_cnt: int
    subject_id: int
    topic_list: str
    chapter_id: int

class PlannerQuestionSelection(BaseModel):
    student_id: int
    exam_id: int
    chapter_id: int
