import os
import traceback
from http import HTTPStatus
from typing import List

import firebase_admin
import pandas as pd
from fastapi import APIRouter
from schemas.Notification import UpdateToken,TestNotification
import datetime
from firebase_admin import messaging
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from datetime import datetime,timedelta
router = APIRouter(
    prefix='/api',
    tags=['Notification'],
)

@router.post('/update_student_token', description='Update student token', status_code=201)
async def update_student_token(inputData: UpdateToken):
    try:
        conn = Tortoise.get_connection("default")
        user_id = inputData.user_id
        token=inputData.token
        query = f"update student_users set fcm_token='{token}' where id={user_id}"
        result=await conn.execute_query_dict(query)
        print(result)
        return JSONResponse(status_code=200,content={"message": "Token saved successfully", "response": user_id,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={'error': f"{e}","success":False})

@router.get("/send-notification/{notification_id}",description='Send notification to users', status_code=201)
async def send_notification(notification_id:int=0):
    # [START send_multicast_error]
    # These registration tokens come from the client FCM SDKs.
    try:
        conn = Tortoise.get_connection("default")
        query = f"select type,exam_id,subject_id,student_id,individual_email,title,message from notification where id={notification_id}"
        result = await conn.execute_query_dict(query)
        resultdf=pd.DataFrame(result)
        failed_tokens = []
        tokens=[]
        if result:
            print("sending notifications")
            result=result[0]
            exam_id=result['exam_id']
            student_id=result['student_id']
            title=result['title']
            message=result['message']
            print(f"exam_id {exam_id}")
            #####Send notification to all users having exam id##############333
            if exam_id!=0:
                examquery=f'SELECT id as user_id,fcm_token FROM student_users where grade_id={exam_id};'
                result = await conn.execute_query_dict(examquery)
                #print(result)
                tokendf=pd.DataFrame(result)
                users=[d['user_id'] for d in result if 'user_id' in d]
                #print(users)
                tokendf=tokendf.dropna()
                tokens=tokendf['fcm_token'].to_list()
                for id in users:
                    insertquery = f'insert into notification_history (student_id,notification_id) values({id},{notification_id})'
                    await conn.execute_query_dict(insertquery)
                #tokens=[d['fcm_token'] for d in result if 'fcm_token' in d]
                #print(tokens)
                if tokens:
                    message = messaging.MulticastMessage(
                        data={'title': title, 'body': message,
                              'time': datetime.now().strftime("%d-%m-%y %H:%M:%S")
                              },
                        tokens=tokens,
                    )
                    response = messaging.send_multicast(message)
                    if response.failure_count > 0:
                        responses = response.responses
                        for idx, resp in enumerate(responses):
                            if not resp.success:
                                # The order of responses corresponds to the order of the registration tokens.
                                failed_tokens.append(tokens[idx])
                        # print('List of tokens that caused failures: {0}'.format(failed_tokens))
                    else:
                        return JSONResponse(status_code=400,
                                            content={'message': "Please enter a valid notification id", "success": False})

                    # [END send_multicast_error]
                    return JSONResponse(status_code=200,
                                        content={'message': "Notifications sent to {0} people".format(len(tokens) - len(
                                            failed_tokens)),
                                                 'failed': 'List of tokens that caused failures: {0}'.format(failed_tokens),
                                                 "success": True})
            if student_id!=0 and student_id is not None:
                studentquery=f'SELECT id as user_id,fcm_token FROM student_users where fcm_token is not null and id={student_id};'
                result = await conn.execute_query_dict(studentquery)
                tokens=[]
                tokendf=pd.DataFrame(result)
                tokendf = tokendf.dropna()
                if not tokendf.empty:
                    tokens = tokendf['fcm_token'].to_list()
                users=[d['user_id'] for d in result if 'user_id' in d]
                #tokens=[d['fcm_token'] for d in result if 'fcm_token' in d]
                #print(tokens)

                for student_id in users:
                    #student_id=tokendict['user_id']
                    insertquery = f'insert into notification_history (student_id,notification_id) values({student_id},{notification_id})'
                    await conn.execute_query_dict(insertquery)
                if tokens:
                    message = messaging.MulticastMessage(
                    data={'title': title, 'body': message,
                          'time': datetime.now().strftime("%d-%m-%y %H:%M:%S")
                          },
                    tokens=tokens,
                    )
                    response = messaging.send_multicast(message)
                    if response.failure_count > 0:
                        responses = response.responses
                        for idx, resp in enumerate(responses):
                            if not resp.success:
                                # The order of responses corresponds to the order of the registration tokens.
                                failed_tokens.append(tokens[idx])
                        #print('List of tokens that caused failures: {0}'.format(failed_tokens))
            else:
                return JSONResponse(status_code=400,
                                    content={'message': "Please enter a valid notification id", "success": False})

            # [END send_multicast_error]
            return JSONResponse(status_code=200, content={'message': "Notifications sent to {0} people".format(len(tokens) - len(
                failed_tokens)), 'failed': 'List of tokens that caused failures: {0}'.format(failed_tokens), "success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})

@router.post("/send-test-notification", description='Send Test notification to a user', status_code=201)
async def send_notification(notification:TestNotification):
    try:
        # [START send_multicast_error]
        # These registration tokens come from the client FCM SDKs.
        token=notification.token
        heading=notification.heading
        body=notification.body
        """
        message = messaging.Message(
            notification=messaging.Notification(
                title=heading,
                body=body,
            ),
            token=token,
        )
        """
        message = messaging.Message(
            data={
                'title': heading,
                'body': body,
                'time':datetime.now().strftime("%d-%m-%y %H:%M:%S")
            },
            token=token,
        )
        messaging.send(message)
        return JSONResponse(status_code=200, content={'message': "Notification sent", "success": True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})


@router.get('/notification-history/{user_id}',description='get last 10 notification for user')
async def notificationHistory(user_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f"SELECT nh.student_id,nh.notification_id,nh.createdAt as notification_date," \
                f"n.title,n.message FROM notification_history AS nh INNER JOIN  notification AS n ON nh.notification_id=n.id" \
                f"  WHERE nh.student_id={user_id} ORDER BY n.notification_date DESC limit 10"
        res = await conn.execute_query_dict(query)
        if res:
            res=pd.DataFrame(res)
            res['notification_date']=res['notification_date'].dt.strftime("%d-%m-%y")
            res=res.to_dict("records")
        if not res:
            return JSONResponse(status_code=400,
                                content={"response": "no past notification for this user", "success": False})
        else:
            return JSONResponse(status_code=200, content={"response": res, "success": True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})

@router.delete('/clear-notifications/{student_id}', description='delete old notifications', status_code=201)
async def removeNotifications(student_id: int=0):
    start_time=datetime.now()
    conn = Tortoise.get_connection("default")
    try:
        query = 'delete from notification_history where student_id={} '.format(
            student_id)
        value = await conn.execute_query_dict(query)
        print(f"execution time:{datetime.now()-start_time}")
        return JSONResponse(status_code=200,content={'message': 'notifications deleted successfully', 'success': True})
    except Exception as e:
        return JSONResponse(status_code=400,content={'error': f'{e}', 'success': False})