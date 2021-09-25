import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
import redis
import json
from fastapi.encoders import jsonable_encoder
from db.engine import db_connection
from schemas.SubjectRating import SubjectRating
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from datetime import datetime,timedelta
router = APIRouter(
    prefix='/api',
    tags=['Exams'],
)

@router.get('/get-all-exams', description='Get All Exams', status_code=201)
async def getAllExams():
    try:
        conn = Tortoise.get_connection("default")
        r=redis.Redis()
        class_exams=[]
        if r.exists("class_exams"):
            class_exams=json.loads(r.get("class_exams"))

        else:
            query = 'select id,class_exam_cd from class_exams'
            class_exams = await conn.execute_query_dict(query)
            r.setex("class_exams", timedelta(days=1), json.dumps(class_exams))
        resp={
            'message': "Class/Exam List",
            'response': class_exams,
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/subjects/{exam_id}",description='Get All Subjects From An Exam', status_code=201)
async def GetAllSubjects(exam_id:int=0):
    try:
        start_time=datetime.now()
        conn = Tortoise.get_connection("default")
        r=redis.Redis()
        exam_cache={}
        subjects=''
        if r.exists(str(exam_id)+"_examid"):
            exam_cache=json.loads(r.get(str(exam_id)+"_examid"))
            if "subjects" in exam_cache:
                subjects=exam_cache['subjects']
            else:
                query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} group by exam_subjects.subject_id'
                subjects = await conn.execute_query_dict(query)
                exam_cache['subjects'] = subjects
                r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))
        else:
            query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} group by exam_subjects.subject_id'
            subjects = await conn.execute_query_dict(query)
            if len(subjects) == 0:
                return JSONResponse(status_code=400, content={"msg": f"no subjects with the given exam_id : {exam_id}",
                                                              "success": False})
            exam_cache['subjects'] = subjects
            r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))

        print(f"execution time {datetime.now() - start_time}")
        return JSONResponse(status_code=200,content={'message': "Subject list by class Id", 'response':subjects,"success":True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/topics-by-subject-id/{subject_id}",description='Get All Topics From A Subject', status_code=201)
async def GetAllTopics(subject_id:int=0):
    try:
        start_time=datetime.now()
        conn = Tortoise.get_connection("default")
        r=redis.Redis()
        subject_cache={}
        topics=''
        if r.exists(str(subject_id)+"_subject_id"):
            subject_cache=json.loads(r.get(str(subject_id)+"_subject_id"))
            if "topics" in subject_cache:
                topics=subject_cache['topics']
            else:
                query = f"select id as topic_id,topic_name from topics where subject_id={subject_id}"
                topics = await conn.execute_query_dict(query)
                subject_cache['topics']=topics
                r.setex(str(subject_id)+"_subject_id",timedelta(days=1),json.dumps(subject_cache))
        else:
            query = f"select id as topic_id,topic_name from topics where subject_id={subject_id}"
            topics = await conn.execute_query_dict(query)
            if len(topics) == 0:
                return JSONResponse(status_code=400, content={"msg": f"no topics with given subject_id : {subject_id}",
                                                              "success": False})
            subject_cache['topics'] = topics
            r.setex(str(subject_id) + "_subject_id", timedelta(days=1), json.dumps(subject_cache))


        print(f"execution time:{datetime.now()-start_time}")
        return JSONResponse(status_code=200,content={'message': "Topic list by subject Id",
                                                     'response': topics,
                                                     "success":True
                                                     })
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/topics-by-chapter-id/{student_id}/{chapter_id}",description='Get All Topics From A Chapter', status_code=201)
async def GetAllTopics(student_id:int=0,chapter_id:int=0):
    try:
        start_time=datetime.now()
        conn = Tortoise.get_connection("default")
        r=redis.Redis()
        chapter_cache={}
        topics=''
        class_exam_id=""
        if r.exists(str(student_id) + "_sid"):
            student_cache = json.loads(r.get(str(student_id) + "_sid"))
            if "exam_id" in student_cache:
                class_exam_id = student_cache['exam_id']
            else:
                query = f'SELECT grade_id FROM student_users where id={student_id} limit 1;'  # fetch exam_id by user_id
                class_exam_id = await conn.execute_query_dict(query)
                if len(class_exam_id) == 0:
                    resp = {
                        "message": "No exam Found for this user",
                        "success": False
                    }
                    return resp, 400
                class_exam_id = int(class_exam_id[0]['grade_id'])

                student_cache['exam_id'] = class_exam_id
                r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))
        else:
            query = f'SELECT grade_id FROM student_users where id={student_id} limit 1;'  # fetch exam_id by user_id
            class_exam_id = await conn.execute_query_dict(query)
            if len(class_exam_id) == 0:
                resp = {
                    "message": "No exam Found for this user",
                    "success": False
                }
                return resp, 400
            class_exam_id = int(class_exam_id[0]['grade_id'])
            student_cache={'exam_id':class_exam_id}
            r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))


        if r.exists(str(chapter_id)+"_chapter_id"):
            chapter_cache=json.loads(r.get(str(chapter_id)+"_chapter_id"))
            if 'topics' in chapter_cache:
                topics=chapter_cache['topics']
            else:
                query = f"select id ,topic_name from topics where chapter_id={chapter_id} and class_id={class_exam_id}"
                topics = await conn.execute_query_dict(query)
                chapter_cache['topics']=topics
                r.setex(str(chapter_id)+"_chapter_id",timedelta(days=1),json.dumps(chapter_cache))
        else:
            query = f"select id ,topic_name from topics where chapter_id={chapter_id} and class_id={class_exam_id}"
            topics = await conn.execute_query_dict(query)
            if len(topics)!=0:
                chapter_cache['topics'] = topics
                r.setex(str(chapter_id) + "_chapter_id", timedelta(days=1), json.dumps(chapter_cache))
            else:
                return JSONResponse(status_code=400, content={"msg": f"no topics with given Chapter ID : {chapter_id}",
                                                              "success": False})

        print(f"execution time : {datetime.now()-start_time}")
        return JSONResponse(status_code=200,content={'message': "Topic list by Chapter Id", 'response': topics,"success":True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/chapters/{student_id}/{subject_id}",description='Get All Chapters From A Subject', status_code=201)
async def GetAllChapters(student_id:int=0,subject_id:int=0):
    try:
        start_time=datetime.now()
        r=redis.Redis()
        conn = Tortoise.get_connection("default")
        subject_cache={}
        chapters=''
        class_exam_id=""
        if r.exists(str(student_id) + "_sid"):
            student_cache = json.loads(r.get(str(student_id) + "_sid"))
            if "exam_id" in student_cache:
                class_exam_id = student_cache['exam_id']
            else:
                query = f'SELECT grade_id FROM student_users where id={student_id} limit 1;'  # fetch exam_id by user_id
                class_exam_id = await conn.execute_query_dict(query)
                if len(class_exam_id) == 0:
                    resp = {
                        "message": "No exam Found for this user",
                        "success": False
                    }
                    return resp, 400
                class_exam_id = int(class_exam_id[0]['grade_id'])

                student_cache['exam_id'] = class_exam_id
                r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))
        else:
            query = f'SELECT grade_id FROM student_users where id={student_id} limit 1;'  # fetch exam_id by user_id
            class_exam_id = await conn.execute_query_dict(query)
            if len(class_exam_id) == 0:
                resp = {
                    "message": "No exam Found for this user",
                    "success": False
                }
                return resp, 400
            class_exam_id = int(class_exam_id[0]['grade_id'])
            student_cache = {'exam_id': class_exam_id}
            r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))

        if r.exists(str(subject_id)+"_subject_id"):
            subject_cache=json.loads(r.get(str(subject_id)+"_subject_id"))
            if "chapters" in subject_cache:
                chapters=subject_cache['chapters']
            else:
                query = f"select chapter_id,chapter_name from exam_subject_chapters where subject_id={subject_id} and class_exam_id{class_exam_id} and chapter_name is not null"
                chapters = await conn.execute_query_dict(query)
                if len(chapters)!=0:
                    subject_cache['chapters']=chapters
                    r.setex(str(subject_id)+"_subject_id",timedelta(days=1),json.dumps(subject_cache))
                else:
                    return JSONResponse(status_code=400,
                                        content={"msg": f"no Chapters with given subject_id : {subject_id}"})
        else:
            query = f"select chapter_id,chapter_name from exam_subject_chapters where subject_id={subject_id} and class_exam_id={class_exam_id} and chapter_name is not null"
            chapters = await conn.execute_query_dict(query)
            if len(chapters) != 0:
                subject_cache['chapters'] = chapters
                r.setex(str(subject_id) + "_subject_id", timedelta(days=1), json.dumps(subject_cache))
            else:
                return JSONResponse(status_code=400,
                                    content={"msg": f"no Chapters with given subject_id : {subject_id}"})
        print(f"execution time: {datetime.now()-start_time}")
        return JSONResponse(status_code=200, content={'message': "Chapters list by subject Id", 'response': chapters,"success":True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":True})




@router.get("/subject-topics/{exam_id}",description="Get All Subjects and All Topics in These Subjects From an Exam",status_code=201)
async def GetSubjectTopic(exam_id:int):
    try:
        start_time=datetime.now()
        r=redis.Redis()
        exam_cache={}
        conn = Tortoise.get_connection("default")
        if r.exists(str(exam_id)+"_examid"):
            exam_cache=json.loads(r.get(str(exam_id)+"_examid"))
            if "subject_topics" in exam_cache:
                subject_topics=exam_cache['subject_topics']
            else:
                query = f'select esc.subject_id as subject_id,s.subject_name as subject_name,st.id as topic_id,st.topic_name as topic_name from subjects s join topics st on s.id=st.subject_id join exam_subject_chapters esc on esc.subject_id=s.id where esc.class_exam_id={exam_id} group by subject_name,topic_name'
                subject_topics = await conn.execute_query_dict(query)
                exam_cache['subject_topics']=subject_topics
                r.setex(str(exam_id)+"_examid",timedelta(days=1),json.dumps(exam_cache))
        else:
            query = f'select esc.subject_id as subject_id,s.subject_name as subject_name,st.id as topic_id,st.topic_name as topic_name from subjects s join topics st on s.id=st.subject_id join exam_subject_chapters esc on esc.subject_id=s.id where esc.class_exam_id={exam_id} group by subject_name,topic_name'
            subject_topics = await conn.execute_query_dict(query)
            if len(subject_topics) == 0:
                return JSONResponse(status_code=400, content={"msg": f"no subjects & topics with given exam Id : {exam_id}",
                                                              "success": False})
            exam_cache['subject_topics'] = subject_topics
            r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))
        print(f"execution time : {datetime.now()-start_time}")
        return JSONResponse(status_code=200, content={"message":"subject_topic","response": subject_topics,"success":True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.post('/subject-rating', description='Save Subject Rating')
async def subject_rating(s_data:SubjectRating):
    conn = Tortoise.get_connection("default")
    getJson = jsonable_encoder(s_data)
    subjects_rating=getJson['subjects_rating']
    print(subjects_rating)
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