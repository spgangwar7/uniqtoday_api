from pydantic import BaseModel

class BookmarkQuestion(BaseModel):
    student_id:int
    exam_id:int
    subject_id:int
    question_id:int
    chapter_id:int