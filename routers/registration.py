import json
from http import HTTPStatus
from typing import List
import pandas as pd
import traceback

import redis
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
import os
import math
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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


@router.get('/Otp/{email_or_mobile}', description='Mobile OTP')
async def MobileOtp(email_or_mobile:str):
    import requests
    import random
    conn = Tortoise.get_connection("default")
    user_email=''
    mobile=0
    flag='mobile'
    if '@' in email_or_mobile:
        flag='email'
        user_email=email_or_mobile
        query = f"select mobile from student_users where email='{user_email}' limit 1"
        res=await conn.execute_query_dict(query)
        if not res:
            return JSONResponse(content={"message": "Please sign up first", "success": False})
        mobile=int(res[0]['mobile'])
    else:
        flag='mobile'
        mobile=int(email_or_mobile)
        query = f"select email from student_users where mobile={mobile} limit 1"
        res = await conn.execute_query_dict(query)
        if not res:
            return JSONResponse(content={"message": "Please sign up first", "success": False})
        user_email = str(res[0]['email'])


    try:
        mobile_otp = random.randint(10000, 99999)
        if mobile==9999999999:
            mobile_otp=11111
        else:
            username = 'ThomsonDigital'
            password = 'Dv0-!dQ3'
            senderId = 'TDUNIQ'  # fixed
            massageTemplateId = '1507161742688982797'  # fixed

            message = str(
                mobile_otp) + " is your UNIQ verification code valid for 10 minutes only, one time use. Please DO NOT share this OTP with anyone to ensure account's security."

            url = f"http://smsjust.com/sms/user/urlsms.php?username={username}&pass={password}&senderid={senderId}&message={message}&dest_mobileno={mobile}&msgtype=TXT&dlttempid={massageTemplateId}&response=Y"
            response = requests.get(url)
            query = f'update student_users set mobile_otp={mobile_otp} where mobile={mobile}'
            await conn.execute_query_dict(query)

        """
        # Commenting code for testing purpose, static OTP 12345 will be sent
        class Object(object):
            pass

        response = Object()
        response.status_code = 200
        mobile_otp = 12345
        """
        user_name = 'admin.unitrack@thomsondigital.com'
        password = 'Yug08091'
        mail_from = user_name
        mail_to = user_email
        mail_subject = 'login otp'
        mail_body = str(mobile_otp) + " is your otp"
        mimemsg = MIMEMultipart()
        mimemsg['From'] = mail_from
        mimemsg['To'] = mail_to
        mimemsg['Subject'] = mail_subject
        mimemsg.attach(MIMEText(mail_body, 'plain'))
        connection = smtplib.SMTP(host='smtp.office365.com', port=587)
        connection.starttls()
        connection.login(user_name, password)
        connection.send_message(mimemsg)
        connection.quit()
        query = f'update student_users set email_otp={mobile_otp} where email="{user_email}"'
        await conn.execute_query_dict(query)



    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})

    try:
        resp = {
            "success": True,
            "otp": mobile_otp,
            "message": "Login Otp"
        }

        return JSONResponse(status_code=200, content=resp)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})



@router.post('/studentlogin', description='Student Login')
async def studentloginElast(data: StudentLogin):
    try:
        start_time = datetime.now()
        conn = Tortoise.get_connection('default')
        email_or_mobile = data.email_or_mobile
        mobile_otp=data.otp
        email_otp=data.otp
        mobile=0
        email=''
        if '@' in email_or_mobile:
            email=email_or_mobile
        else:
            mobile=int(email_or_mobile)


        r = redis.Redis()
        query = f'select id,first_name,user_name,last_name,email,mobile,address,city,state,gender,status,mobile_otp,stream_code,grade_id from student_users where (mobile_otp={mobile_otp} or email_otp={email_otp}) and (mobile={mobile} or email="{email}")'
        val = await conn.execute_query_dict(query)
        val = pd.DataFrame(val)
        user_id = val["id"].iloc[0]
        val = val.rename(columns={"grade_id": "exam_id"})
        login_cache = {}
        if r.exists(str(user_id) + "_sid"):
            pass
        else:
            value = val[['first_name', "user_name", "last_name", "exam_id"]]
            login_cache = value.to_dict('records')
            r.set(str(user_id) + "_sid", json.dumps(login_cache))
        if val.empty:
            return JSONResponse(status_code=400, content={"message": "Please sign up first", "success": False})
        time_taken = datetime.now() - start_time
        print(time_taken.total_seconds())
        return JSONResponse(status_code=200,
                            content={"message": "You are logged in", "result": val.to_dict('records'), "success": True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})
