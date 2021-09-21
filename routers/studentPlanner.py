import json
import traceback
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import List
import pandas as pd
from tortoise import Tortoise
from tortoise.queryset import QuerySet
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from tortoise.query_utils import Q
from fastapi.responses import JSONResponse
from schemas.StudentPlanner import StudentPlanners
router = APIRouter(
    prefix='/api',
    tags=['Student Planner'],
)

@router.post('/student-planner', description='save student planner details ')
async def student_planner(studentp: StudentPlanners):
    #print(studentp)
    try:
        conn = Tortoise.get_connection("default")
        student_id=studentp.student_id
        fromdate = datetime.strptime(studentp.date_from,"%Y-%m-%d").date()
        todate = datetime.strptime(studentp.date_to,"%Y-%m-%d").date()
        #check if planner already exists for current week in db
        checkquery=f'SELECT id FROM student_planner where date_from= "{fromdate}" and date_to="{todate}" and student_id={student_id};'
        records=await conn.execute_query_dict(checkquery)
        #print("Records length: "+str(len(records)))
        if len(records)!=0:
            resp={
                "message":"Planner already exists for the user for this week",
                "success":False
            }
            return resp
        exam_id=studentp.exam_id
        chapter_id=json.loads(studentp.chapter_id)
        chapter_id_list = [int(i) for i in chapter_id]
        question_count=30
        timequery=f'SELECT exam_time_per_ques FROM class_exams where id={exam_id};'
        timeperques=await conn.execute_query_dict(timequery)
        timeperques=timeperques[0]
        timeperques=timeperques['exam_time_per_ques']
        test_time_in_min=question_count*timeperques
        print(test_time_in_min)
        if len(chapter_id_list) == 1:
            cid = chapter_id_list[0]
            subjectquery=f'SELECT subject_id FROM exam_subject_chapters where chapter_id= {cid};'
            subject_id=await conn.execute_query_dict(subjectquery)
            subject_id = int(subject_id[0]['subject_id'])

            query = f"insert into student_planner(student_id,exam_id,subject_id,question_count,test_time_in_min, \
                          chapter_id,date_from,date_to)values({student_id}, \
                          {exam_id},{subject_id}, {question_count}, {test_time_in_min}, \
                          {cid},'{fromdate}','{todate}')"
            result = await conn.execute_query_dict(query)
        else:
            for cid in chapter_id_list:
                #print(cid)
                subjectquery = f'SELECT subject_id FROM exam_subject_chapters where chapter_id= {cid};'
                subject_id = await conn.execute_query_dict(subjectquery)
                subject_id = int(subject_id[0]['subject_id'])
                query = f"insert into student_planner(student_id,exam_id,subject_id,question_count,test_time_in_min, \
                              chapter_id,date_from,date_to)values({student_id}, \
                              {exam_id},{subject_id}, {question_count}, {test_time_in_min}, \
                              {cid},'{fromdate}','{todate}')"
                result = await conn.execute_query_dict(query)

        return JSONResponse(status_code=200,content={"message": "Student planner saved successfully","success":True})

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"message": "Error occured Student planner not saved","success":False})

@router.get('/student-planner/{student_id}', description='Get student planner details ')
async def get_student_planner(student_id:int=0):
    try:

        conn = Tortoise.get_connection("default")
        query=f"select * from student_planner where student_id={student_id}"
        result= await conn.execute_query_dict(query)
        resp={
            "result": result,
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"message": "Error occured record not found","success":False})

@router.get('/student-planner-current-week/{student_id}', description='Get student planner details for current week')
async def get_student_planner(student_id:int=0):
    try:
        now = datetime.now()
        monday = now - timedelta(days=now.weekday())
        #print(monday.date())
        conn = Tortoise.get_connection("default")
        query=f'select sp.id,sp.exam_id,sp.subject_id,question_count,test_time_in_min,sp.chapter_id, esc.chapter_name ,date_from,date_to from student_planner as sp join exam_subject_chapters as esc on sp.chapter_id=esc.chapter_id where student_id={student_id} and date_from="{monday.date()}"'
        result= await conn.execute_query_dict(query)
        result=pd.DataFrame(result)
        if result.empty:
            return JSONResponse(status_code=400,content={"message": "Planner does not exist for this user", "success": False})
        else:
            result["date_from"] = pd.to_datetime(result["date_from"]).dt.strftime('%Y-%m-%d')
            result["date_to"] = pd.to_datetime(result["date_to"]).dt.strftime('%Y-%m-%d')

        #print(result)
        result=result.fillna(0)
        resp={
            "result": result.to_dict("records"),
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"message": "Error occured record not found","success":False})
