import traceback
from datetime import datetime,time,date
from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import IntegrityError
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from schemas.Payment import OrderSchema,VerifyPayment
from pydantic import BaseModel
from typing import Optional
from dateutil.relativedelta import relativedelta

import razorpay
import random, string

router = APIRouter(
    prefix='/api/payment',
    tags=['Payment'],
)

@router.post('/order-id', description='Get Order ID')
async def get_order_id(orderdetails:OrderSchema):
    try:
        _RAZORPAY_KEY = "rzp_test_foHLtdKSJjEDzv"
        _RAZORPAY_SECRET = "RFrAe68CEzVrQpuuHnlKJHcy"
        client = razorpay.Client(auth=(_RAZORPAY_KEY, _RAZORPAY_SECRET))
        order_amount=orderdetails.amount*100
        order_currency=orderdetails.currency
        order_notes=orderdetails.notes
        order_receipt = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
        print(order_receipt)

        orderid=client.order.create(data={"amount":order_amount,"currency":order_currency,"notes":order_notes
                                          ,"receipt":order_receipt})
        print(orderid)

        return {'order_details': orderid,'success':True}
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})


@router.post('/verify-payment', description='Get Order ID')
async def verify_payment(verifyPayment:VerifyPayment):
    try:
        conn = Tortoise.get_connection("default")
        _RAZORPAY_KEY = "rzp_test_ZDZNbJZXjNvae9"
        _RAZORPAY_SECRET = "AeB0cSCNc2fhU7fJZx4nRvYc"
        client = razorpay.Client(auth=(_RAZORPAY_KEY, _RAZORPAY_SECRET))
        razorpay_payment_id=verifyPayment.payment_id
        razorpay_order_id=verifyPayment.order_id
        razorpay_signature=verifyPayment.signature
        user_id=verifyPayment.user_id
        orderDetails=client.payment.fetch(razorpay_payment_id)
        params_dict = {
            'razorpay_order_id': razorpay_order_id,
            'razorpay_payment_id': razorpay_payment_id,
            'razorpay_signature': razorpay_signature
        }
        notes=orderDetails['notes']
        month=notes['month']
        exam_id=notes['exam_id']
        payment_date=datetime.fromtimestamp(orderDetails['created_at'])
        subscription_start_date=date.fromtimestamp(orderDetails['created_at'])
        expiry_date=date.today() + relativedelta(months=+int(month))
        transaction_id=orderDetails['id']
        order_id=orderDetails['order_id']
        order_status=orderDetails['status']
        transaction_status="Pass"
        subscription_type="P"
        payment_by=orderDetails["method"]
        amount=orderDetails["amount"]/100
        exam_year=date.today().year
        query = f'insert into users_purchase (user_id,purchase_date,exam_year,subscription_id,subscription_start_date,subscription_end_date,\
                subscription_type,amount,transaction_id,order_id,order_status,transaction_status,payment_by,created_on) values \
                ("{user_id}","{subscription_start_date}","{exam_year}","{exam_id}","{subscription_start_date}","{expiry_date}","{subscription_type}", \
                 "{amount}","{transaction_id}","{order_id}","{order_status}","{transaction_status}","{payment_by}","{subscription_start_date}")'
        result = await conn.execute_query_dict(query)

        query1 = f'update student_users set grade_id="{exam_id}" where id="{user_id}"'
        result1 = await conn.execute_query_dict(query1)
        query2 = f'update student_preferences set subscription_yn="Y",subscription_expiry_date="{expiry_date}" where student_id="{user_id}"'
        result2 = await conn.execute_query_dict(query2)

        status = client.utility.verify_payment_signature(params_dict)
        return {'orderDetails': orderDetails,'verification_status':status,'success':True}
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})
