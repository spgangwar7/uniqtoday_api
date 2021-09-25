import traceback
from http import HTTPStatus
import random
from typing import List
import pandas as pd
from fastapi import APIRouter,BackgroundTasks
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import IntegrityError
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from tortoise.models import Model
from tortoise.transactions import in_transaction
from schemas.Referrals import ReferStudent,UpdateReferStudent,SendReferralEmail
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
router = APIRouter(
    prefix='/api',
    tags=['Referrals'],
)

@router.post('/insert-referr-student', description='Insert referral list by user id')
async def post(refer_student:ReferStudent,background_tasks: BackgroundTasks):
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
                        import random, string
                        referrral_code = ''.join(
                            random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in
                            range(8))
                        #referrral_code=f'UNIQ-{student_id}'
                        print(referrral_code)
                        query = f"insert into student_referrals(student_id,referral_name,referral_email,referral_phone,status,referral_code) values({student_id},'', '{email}', '',{2}, '{referrral_code}')"
                        await conn.execute_query_dict(query)
                        inserted_emails_list.append(email)
                        link = f'https://app.uniqtoday.com/referral/{referrral_code}'
                        referral_data = {"receiver_email": email, "sender_user_id": student_id, "link": link}
                        data=SendReferralEmail(**referral_data)
                        await sendReferEmail(data)
            return JSONResponse(status_code=200,content={'message': 'Thanks for your reference',"new_referrals":inserted_emails_list,"duplicate_referrals":existing_email_list, 'success': True})
        else:
            if student_id and exam_id  and len(email) > 0 and len(email) <= 255:
                check_query=f'SELECT * FROM student_referrals where referral_email="{email}"'
                result=await conn.execute_query_dict(check_query)
                import random, string
                referrral_code = ''.join(
                    random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in
                    range(8))
                #referrral_code = f'UNIQ-{student_id}'
                if result:
                    return JSONResponse(status_code=400,content={'message': 'User already exists', 'error': 'User already exists','success': False})
                else:
                    query = f"insert into student_referrals(student_id,referral_name,referral_email,referral_phone,status,referral_code) values({student_id},'', '{email}', '', {2}, '{referrral_code}')"
                    await conn.execute_query_dict(query)
                    link=f'https://app.uniqtoday.com/referral/{referrral_code}'
                    referral_data={"receiver_email":email,
                                   "sender_user_id":student_id
                        ,"link":link}
                    data = SendReferralEmail(**referral_data)
                    await sendReferEmail(data)
                return JSONResponse(status_code=200,content={'message': 'Thanks for your reference', 'success': True})
    except Exception as e:
        return JSONResponse(status_code=400,content={'message': 'something wrong',
                                                     'error': '{}'.format(e),
                                                     'success': False})
        print(e)
        traceback.print_tb(e.__traceback__)

@router.put('/update-referr-student', description='Update referral list by user id')
async def put(refer_student:UpdateReferStudent):
    conn = Tortoise.get_connection("default")
    try:
        student_id=refer_student.student_id
        email=refer_student.email
        user_name=refer_student.user_name
        phone=refer_student.phone
        referral_code = refer_student.referral_code
        if len(email) > 0 and len(email) <= 255:
            check_query = f'SELECT id FROM student_referrals where referral_email="{email}" and referral_code="{referral_code}"'
            result = await conn.execute_query_dict(check_query)
            if result:
                update_query=f"UPDATE student_referrals SET  `referral_name` = '{user_name}', `referral_phone` = '{phone}',`status`={1}  WHERE referral_email='{email}' and referral_code='{referral_code}';"
                await conn.execute_query_dict(update_query)
            else:
                query = f"insert into student_referrals(student_id,referral_email,referral_phone,status,referral_code) values({student_id}, '{email}',{phone}, {1}, '{referral_code}')"
                await conn.execute_query_dict(query)
            return JSONResponse(status_code=200,content={'message': 'Thanks for your reference',"new_referrals":email, 'success': True})

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={'message': 'something wrong',
                                                     'error': '{}'.format(e),
                                                     'success': False})


async def send_mail(receiver_email,mail_subject,mail_body):
    try:
        user_name = 'admin.unitrack@thomsondigital.com'
        password = 'Yug08091'
        mimemsg = MIMEMultipart()
        mimemsg['From'] = user_name
        mimemsg['To'] = receiver_email
        mimemsg['Subject'] = mail_subject
        mimemsg.attach(MIMEText(mail_body, 'plain'))
        connection = smtplib.SMTP(host='smtp.office365.com', port=587)
        connection.starttls()
        connection.login(user_name, password)
        connection.send_message(mimemsg)
        connection.quit()
        return True
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'message': 'something wrong',
                                                      'error': '{}'.format(e),
                                                      'success': False})


@router.post('/send-referral-email',description='send referral email')
async def sendReferEmail(input_data:SendReferralEmail):
    try:
        conn=Tortoise.get_connection('default')
        sender_user_id=input_data.sender_user_id
        receiver_email=input_data.receiver_email
        link=input_data.link
        query=f"select user_name from student_users where id={sender_user_id} limit 1"
        res=await conn.execute_query_dict(query)
        sender_name=res[0]['user_name']
        mail_subject = 'Referral Link'
        mail_body = f"You are referred by {sender_name}." \
                    f"\n You can register by signing up on the following link." \
                    f"\n click here :{link}"
        resp=''
        if await send_mail(receiver_email, mail_subject, mail_body):
            resp = {
                "response": {
                    "sender_user_id": sender_user_id,
                    "sender_user_name": sender_name,
                    "reciever_email": receiver_email,
                    "referral_link": link
                },
                "message": "referral link sent",
                "success": True

            }
            return JSONResponse(status_code=200, content=resp)
        else:
            resp = {
                "response": "something went wrong",
                "message": "referral link not  sent",
                "success": False
            }
            return JSONResponse(status_code=400,content=resp)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'message': 'something wrong',
                                                      'error': '{}'.format(e),
                                                      'success': False})

