from pydantic import BaseModel
from typing import Optional

class OrderSchema(BaseModel):
    amount :int
    currency :str
    notes: Optional[dict]=None

class VerifyPayment(BaseModel):
    payment_id :str
    order_id :str
    signature: str
    user_id:int