from http import HTTPStatus
from typing import List
import pandas as pd
import traceback
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import *
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from tortoise.models import Model
from tortoise.transactions import in_transaction
from datetime import datetime, time
from schemas.Registeration import Register
import random
from schemas.Registeration import StudentSignup
from elasticsearch import Elasticsearch
from schemas.Registeration import StudentLogin
import numpy as np

router = APIRouter(
    prefix='/api',
    tags=['Registeration and Login'],
)


def mobile_Otp(mobile_no):
    try:
        import requests

        username = 'ThomsonDigital'
        mobile_otp = random.randint(10000, 99999)
        password = 'Dv0-!dQ3'
        senderId = 'TDUNIQ'  # fixed
        massageTemplateId = '1507161742688982797'  # fixed

        message = str(
            mobile_otp) + " is your UNIQ verification code valid for 10 minutes only, one time use. Please DO NOT share this OTP with anyone to ensure account's security."

        url = f"http://smsjust.com/sms/user/urlsms.php?username={username}&pass={password}&senderid={senderId}&message={message}&dest_mobileno={mobile_no}&msgtype=TXT&dlttempid={massageTemplateId}&response=Y"

        response = requests.get(url)
        print(response)

        """
        # Commenting code for testing purpose, static OTP 12345 will be sent
        class Object(object):
            pass

        response = Object()
        response.status_code = 200
        mobile_otp = 12345
        """
        if response.status_code == 200:
            return JSONResponse(status_code=200,
                                content={"mobile_otp": mobile_otp, "msg": "Login Otp", "success": True})
        return JSONResponse(status_code=400, content={"success": False})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f"{e}", "success": False})


@router.post('/register-otp', description='Get OTP for registration')
async def create_user(data: Register):
    start_time = datetime.now()
    conn = Tortoise.get_connection("default")
    try:
        query = f"select email,mobile from student_users where email='{data.email}' or mobile={data.mobile}"
        df1 = await conn.execute_query_dict(query)
        df = pd.DataFrame(df1)
        if not df.empty:
            resp = {
                "message": 'email or mobile already exist',
                "success": False
            }
            return JSONResponse(status_code=400, content=resp)
        return mobile_Otp(data.mobile)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f"{e}", "success": False})


@router.post('/student-signup', description='Student Signup')
async def store_signup_data(s_data: StudentSignup):
    conn = Tortoise.get_connection("default")
    try:
        query = f'insert into student_users(user_name,email,mobile)values("{s_data.user_name}","{s_data.email}",{s_data.mobile})'
        result = await conn.execute_query_dict(query)
        query_studentId = "SELECT id FROM student_users ORDER BY id DESC LIMIT 1"
        studentIdList = await conn.execute_query_dict(query_studentId)
        studentId = int(studentIdList[0].get("id"))
        query2 = f'insert into student_preferences (student_id) values({studentId})'
        await conn.execute_query_dict(query2)
        resp = {
            "message": "student registered successfully",
            "studentID": studentId,
            "success": True
        }
        return JSONResponse(status_code=200, content=resp)
    except Exception as e:
        print(e)
        resp = {
            "message": "student already registered",
            "success": False
        }
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content=resp)


@router.get('/MobileOtp/{mobile_no}', description='Mobile OTP')
async def MobileOtp(mobile_no: int):
    import requests
    import random
    conn = Tortoise.get_connection("default")
    try:
        query = f"select email,mobile from student_users where mobile={mobile_no}"
        val = await conn.execute_query_dict(query)
        if not val:
            return JSONResponse(content={"message":"Please sign up first","success": False})


        username = 'ThomsonDigital'
        mobile_otp = random.randint(10000, 99999)
        password = 'Dv0-!dQ3'
        senderId = 'TDUNIQ'  # fixed
        massageTemplateId = '1507161742688982797'  # fixed

        message = str(
            mobile_otp) + " is your UNIQ verification code valid for 10 minutes only, one time use. Please DO NOT share this OTP with anyone to ensure account's security."

        url = f"http://smsjust.com/sms/user/urlsms.php?username={username}&pass={password}&senderid={senderId}&message={message}&dest_mobileno={mobile_no}&msgtype=TXT&dlttempid={massageTemplateId}&response=Y"

        response = requests.get(url)

        """
        # Commenting code for testing purpose, static OTP 12345 will be sent
        class Object(object):
            pass

        response = Object()
        response.status_code = 200
        mobile_otp = 12345
        """
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})

    if response.status_code == 200:
        try:
            query = f'update student_users set mobile_otp={mobile_otp} where mobile={mobile_no}'
            await conn.execute_query_dict(query)
            resp = {
                "success": True,
                "mobile_otp": mobile_otp,
                "message": "Login Otp"
            }
            return JSONResponse(status_code=200, content=resp)
        except Exception as e:
            print(e)
            traceback.print_tb(e.__traceback__)
            return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})
    return JSONResponse(status_code=response.status_code, content={"success": False})


@router.post('/studentlogin', description='Student Login')
async def studentloginElast(data: StudentLogin):
    try:
        conn = Tortoise.get_connection('default')
        getJson = jsonable_encoder(data)
        df_j = pd.DataFrame([getJson])
        mobile_otp = df_j["mobile_otp"].iloc[0]
        mobile = df_j["mobile"].iloc[0]
        query = f'select id,first_name,user_name,last_name,email,mobile,address,city,state,gender,status,mobile_otp,stream_code,grade_id from student_users where mobile_otp={mobile_otp} and mobile={mobile}'
        val = await conn.execute_query_dict(query)
        if not val:
            return JSONResponse(status_code=400, content={"message": "Please sign up first", "success": False})
        return JSONResponse(status_code=200, content={"message": "You are logged in", "result": val, "success": True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})
