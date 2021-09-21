import traceback
from http import HTTPStatus
import numpy as np
import pandas as pd
from tortoise import Tortoise
from fastapi import APIRouter, HTTPException
from schemas.Tutorial import RegisterTutorial
from fastapi.responses import JSONResponse
import json
from datetime import datetime, timedelta

router = APIRouter(
    prefix='/api',
    tags=['Tutorial'],
)
@router.get("/upcoming-tutorial/{exam_id}/{student_id}",description='get all upcoming tutorials')
async def getUpcomingTutorial(exam_id:int=0,student_id:int=0):
    try:
        conn=Tortoise.get_connection('default')
        today_date=datetime.today().strftime('%y-%m-%d')
        query=f"SELECT id as tutorial_id,exam_id,subject_id,topic_id,session_name,session_desc,session_objectives,session_taken_by," \
              f"DATE_FORMAT(session_date,'%d-%m-%y') as session_date,session_start_time,session_duration,duration_unit FROM student_online_sessions where exam_id={exam_id} and session_date >= '{today_date}'"
        res=await conn.execute_query_dict(query)
        tutorial_list=pd.DataFrame(res)
        if tutorial_list.empty:
            return JSONResponse(status_code=400,content={'message':'No tutorials found','tutorial_list':[],"success":False})
        query=f"SELECT id,student_id, tutorial_id,registered_on,attended_yn FROM student_tutorials where student_id={student_id}"
        registered=await conn.execute_query_dict(query)
        registered_df=pd.DataFrame(registered)
        if not registered_df.empty:
            print(tutorial_list)
            registered_df['registered_yn']="yes"
            output_df=tutorial_list.merge(registered_df,on='tutorial_id',how='left')
            output_df['registered_yn']=output_df['registered_yn'].fillna("no")
            output_df=output_df.fillna("")
            result = {"tutorial_list": output_df.to_dict("records"), "success": True}
            print(output_df)
        else:
            tutorial_list['registered_yn']="no"
            tutorial_list=tutorial_list.fillna("")
            result = {"tutorial_list": tutorial_list.to_dict("records"), "success": True}
        if len(res)==0:
            return JSONResponse(status_code=400,content={'tutorial_list':[],"success":False})
        return result
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={'error': f"{e}","success":False})

@router.post("/student-register-tutorial",description='Register user for a tutorial')
async def registerTutorial(input:RegisterTutorial):
    try:
        conn=Tortoise.get_connection('default')
        student_id=input.student_id
        tutorial_id=input.tutorial_id
        registered_on=datetime.today().strftime('%d-%m-%y')
        query = f"INSERT INTO student_tutorials(student_id,tutorial_id,registered_on,attended_yn)values({student_id},{tutorial_id},'{registered_on}','N')"
        # print(query)
        await conn.execute_query(query)
        result = {"response": "Student successfully registered for tutorial", "success": True}
        return result

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={'error': f"{e}","success":False})