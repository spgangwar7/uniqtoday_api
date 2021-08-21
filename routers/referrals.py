from http import HTTPStatus
import random
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import IntegrityError
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from tortoise.models import Model
from tortoise.transactions import in_transaction
from schemas.Referrals import ReferStudent
router = APIRouter(
    prefix='/api',
    tags=['Referrals'],
)

@router.put('/referr-student', description='Get referral list by user id')
async def put(refer_student:ReferStudent):
    conn = Tortoise.get_connection("default")
    try:
        student_id=refer_student.student_id
        exam_id=refer_student.exam_id
        email=refer_student.email
        if ',' in email:
            emaillist=email.split(",")
            print(emaillist)
            existing_email_list=[]
            inserted_emails_list=[]
            for email in emaillist:
                if student_id and exam_id and len(email) > 0 and len(email) <= 255:
                    check_query = f'SELECT * FROM student_referrals where referral_email="{email}"'
                    result = await conn.execute_query_dict(check_query)
                    if result:
                        existing_email_list.append(email)
                    else:
                        query = f"insert into student_referrals(student_id,exam_grade_id,referral_name,referral_email,referral_phone,status,referral_code) values({student_id}, {exam_id},'', '{email}', '', {2}, {random.randint(10000000, 99999999)})"
                        await conn.execute_query_dict(query)
                        inserted_emails_list.append(email)
            return JSONResponse(status_code=200,content={'message': 'Thanks for your reference',"new_referrals":inserted_emails_list,"duplicate_referrals":existing_email_list, 'success': True})
        else:
            if student_id and exam_id  and len(email) > 0 and len(email) <= 255:
                check_query=f'SELECT * FROM student_referrals where referral_email="{email}"'
                result=await conn.execute_query_dict(check_query)
                if result:
                    return JSONResponse(status_code=200,content={'message': 'User already exists', 'success': True})
                else:
                    query = f"insert into student_referrals(student_id,exam_grade_id,referral_name,referral_email,referral_phone,status,referral_code) values({student_id}, {exam_id},'', '{email}', '', {2}, {random.randint(10000000, 99999999)})"
                    await conn.execute_query_dict(query)
                return JSONResponse(status_code=200,content={'message': 'Thanks for your reference', 'success': True})
    except Exception as e:
        return JSONResponse(status_code=400,content={'message': 'something wrong',
                                                     'error': '{}'.format(e),
                                                     'success': False})