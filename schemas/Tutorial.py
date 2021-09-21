from pydantic.main import BaseModel

class RegisterTutorial(BaseModel):
    student_id:int
    tutorial_id:int
