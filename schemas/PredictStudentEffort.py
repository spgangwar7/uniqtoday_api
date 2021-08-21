from pydantic import BaseModel
class PredictStudentEfforts(BaseModel):
    age: int
    no_of_attempts: int
    previous_test_scores: int
    gender:str
    board_exam_marks_avg:int
    internet_accessibility:str
    private_tuition:str