from pydantic import BaseModel
class QuestionBankQualityCheckTagged(BaseModel):
    start_date: str
    end_date: str
    updated_user: int
    exam_id: int
