import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from db.engine import db_connection
from schemas.SubjectRating import SubjectRating
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
router = APIRouter(
    prefix='/api',
    tags=['Exams'],
)

@router.get('/get-all-exams', description='Get All Exams', status_code=201)
async def getAllExams():
    try:
        conn = Tortoise.get_connection("default")
        query = 'select id,class_exam_cd from class_exams'
        val = await conn.execute_query_dict(query)
        resp={
            'message': "Class/Exam List",
            'response': val,
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/subjects/{exam_id}",description='Get All Subjects From An Exam', status_code=201)
async def GetAllSubjects(exam_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} group by exam_subjects.subject_id'
        val = await conn.execute_query_dict(query)
        #print(val)
        #await conn.close()
        if len(val)==0:
            return JSONResponse(status_code=400,content={"msg":f"no subjects with the given exam_id : {exam_id}","success":False})
        return JSONResponse(status_code=200,content={'message': "Subject list by class Id", 'response':val,"success":True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/topics-by-subject-id/{subject_id}",description='Get All Topics From A Subject', status_code=201)
async def GetAllTopics(subject_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f"select id,topic_name from topics where subject_id={subject_id}"
        val = await conn.execute_query_dict(query)
        if len(val)==0:
            return JSONResponse(status_code=400,content={"msg":f"no topics with given subject_id : {subject_id}",
                                                         "success":False})
        return JSONResponse(status_code=200,content={'message': "Topic list by subject Id",
                                                     'response': val,
                                                     "success":True
                                                     })
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/topics-by-chapter-id/{chapter_id}",description='Get All Topics From A Chapter', status_code=201)
async def GetAllTopics(chapter_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f"select id,topic_name from topics where chapter_id={chapter_id}"
        val = await conn.execute_query_dict(query)
        if len(val)==0:
            return JSONResponse(status_code=400,content={"msg":f"no topics with given Chapter ID : {chapter_id}","success":False})
        return JSONResponse(status_code=200,content={'message': "Topic list by Chapter Id", 'response': val,"success":True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/chapters/{student_id}/{subject_id}",description='Get All Chapters From A Subject', status_code=201)
async def GetAllChapters(student_id:int=0,subject_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f"select chapter_id,chapter_name from exam_subject_chapters where subject_id={subject_id} and chapter_name is not null"
        val = await conn.execute_query_dict(query)
        #print(val)
        #await conn.close()
        if len(val)==0:
            return JSONResponse(status_code=400, content={"msg":f"no Chapters with given subject_id : {subject_id}"})
        return JSONResponse(status_code=200, content={'message': "Chapters list by subject Id", 'response': val,"success":True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":True})



@router.get("/sub-topics/{topic_id}",description="Get All SubTopics From A Topic", status_code=201)
async def GetSubTopic(topic_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query=f"select sub_topic_id,sub_topic_name from subject_sub_topic where topic_id={topic_id}"
        val = await conn.execute_query_dict(query)
        #await conn.close()
        if len(val)==0:
            return JSONResponse(status_code=400, content={"msg":f"no subtopic with the given topic_id : {topic_id}","success":False})
        return JSONResponse(status_code=200, content={"message":"subtopic by topic_id","response":val,"success":True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})

@router.get("/subject-topics/{exam_id}",description="Get All Subjects and All Topics in These Subjects From an Exam",status_code=201)
async def GetSubjectTopic(exam_id:int):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select esc.subject_id as subject_id,s.subject_name as subject_name,st.id as topic_id,st.topic_name as topic_name from subjects s join topics st on s.id=st.subject_id join exam_subject_chapters esc on esc.subject_id=s.id where esc.class_exam_id={exam_id} group by subject_name,topic_name'
        val = await conn.execute_query_dict(query)
        return JSONResponse(status_code=200, content={"message":"subject_topic","response": val,"success":True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.post('/subject-rating', description='Save Subject Rating')
async def subject_rating(s_data:SubjectRating):
    conn = Tortoise.get_connection("default")
    getJson = jsonable_encoder(s_data)
    subjects_rating=getJson['subjects_rating']
    student_id = s_data.student_id
    try:
        query=f"update student_preferences set subjects_rating = '{subjects_rating}' where student_id={student_id}"
        #print(query)
        await conn.execute_query_dict(query)
        return JSONResponse(status_code=200, content={"message":"subjects rating updated successfully","success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"message":"subjects rating not updated ","success":False})