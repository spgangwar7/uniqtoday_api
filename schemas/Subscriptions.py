from pydantic import BaseModel
class SavePurchaseDetails(BaseModel):
    exam_id:int
    student_id:int
    amount:int
    transaction_id:str
    order_id:str
    payment_status:str
    transaction_status:str
    subscription_type:str
    payment_method:str
    payment_date:str
    subscription_start_date:str
    subscription_end_date:str
    subscription_id:int
    exam_year:int

class SaveTrialSubscription(BaseModel):
    student_id:int
    subscription_id:int
    exam_year:int
