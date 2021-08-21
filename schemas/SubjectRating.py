from pydantic.main import BaseModel

class SubjectRating(BaseModel):
    student_id: int
    subjects_rating:str
