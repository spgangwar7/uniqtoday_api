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
        topics=[]
        class_exam_id=""
        if r.exists(str(student_id) + "_sid"):
            student_cache = json.loads(r.get(str(student_id) + "_sid"))
            if "exam_id" in student_cache:
                class_exam_id = student_cache['exam_id']
            else:
                query = f'SELECT grade_id FROM student_users where id={student_id} limit 1;'  # fetch exam_id by user_id
                class_exam_id = await conn.execute_query_dict(query)
                if not class_exam_id:
                    resp = {
                        "message": "No exam Found for this user",
                        "success": False
                    }
                    return JSONResponse(status_code=400,content={"response":resp})
                class_exam_id = int(class_exam_id[0]['grade_id'])
                student_cache={
                    "exam_id":class_exam_id
                }
                r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))
        else:
            query = f'SELECT grade_id FROM student_users where id={student_id} limit 1;'  # fetch exam_id by user_id
            class_exam_id = await conn.execute_query_dict(query)
            if not class_exam_id:
                resp = {
                    "message": "No exam Found for this user",
                    "success": False
                }
                return JSONResponse(status_code=400, content={"response": resp})
            class_exam_id = int(class_exam_id[0]['grade_id'])
            student_cache = {
                "exam_id": class_exam_id
            }
            r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))


        if r.exists(str(chapter_id)+"_chapter_id"):
            chapter_cache=json.loads(r.get(str(chapter_id)+"_chapter_id"))
            if 'topics' in chapter_cache:
                topics=chapter_cache['topics']
                print(f"execution time : {datetime.now() - start_time}")
                return JSONResponse(status_code=200,content={'message': "Topic list by Chapter Id", 'response': topics, "success": True})
            else:
                query=f"SELECT t.id,t.topic_name,sps.student_id," \
                      f" IFNULL((sps.ques_ans_correctly/sps.question_attempted*100),0) AS topic_score,IFNULL(SUM(sk.skill_name='Evaluation'),0) AS E," \
                      f" IFNULL(SUM(sk.skill_name='Comprehension'),0) AS C," \
                      f" IFNULL(SUM(sk.skill_name='Application'),0) AS A," \
                      f" IFNULL(SUM(sk.skill_name='Knowledge'),0) AS K" \
                      f"FROM topics AS t LEFT JOIN student_performance_summary" \
                      f" AS sps ON t.id=sps.topic_id LEFT JOIN skills AS sk ON sk.skill_id=sps.skill_id WHERE t.chapter_id={chapter_id} " \
                      f" AND t.class_id={class_exam_id} GROUP BY id"
                topics = await conn.execute_query_dict(query)
                if not topics:
                    return JSONResponse(status_code=400,
                                        content={"response": "no performance summary for the user", "success": False})
                topics_df = pd.DataFrame(topics)
                filt = topics_df['student_id'] != student_id
                topics_df.loc[filt, 'topic_score'] = 0
                topics_df.loc[filt, "A"] = 0
                topics_df.loc[filt, "E"] = 0
                topics_df.loc[filt, "C"] = 0
                topics_df.loc[filt, "K"] = 0
                topics_df.drop('student_id', axis=1, inplace=True)
                # print(result_df)

                total = topics['A'].sum() + topics['E'].sum() + topics_df['C'].sum() + topics_df['K'].sum()
                if total == 0:
                    total = 1
                topics_df['A'] = ((topics_df['A'] / total) * 100).astype(float).round(2)

                topics_df['E'] = ((topics_df['E'] / total) * 100).astype(float).round(2)
                topics_df['C'] = ((topics_df['C'] / total) * 100).astype(float).round(2)
                topics_df['K'] = ((topics_df['K'] / total) * 100).astype(float).round(2)
                topics_df['topics_score'] = topics_df['topics_score'].astype(float).round(2)
                res=topics_df.to_dict('records')
                chapter_cache={
                    'topics': res
                }
                r.setex(str(chapter_id)+"_chapter_id",timedelta(days=1),json.dumps(chapter_cache))
                print(f"execution time : {datetime.now() - start_time}")
                return JSONResponse(status_code=200,
                                    content={'message': "Topic list by Chapter Id", 'response': res, "success": True})
        else:
            query = f"SELECT t.id,t.topic_name,sps.student_id," \
                    f" IFNULL((sps.ques_ans_correctly/sps.question_attempted*100),0) AS topic_score,IFNULL(SUM(sk.skill_name='Evaluation'),0) AS E," \
                    f" IFNULL(SUM(sk.skill_name='Comprehension'),0) AS C," \
                    f" IFNULL(SUM(sk.skill_name='Application'),0) AS A," \
                    f" IFNULL(SUM(sk.skill_name='Knowledge'),0) AS K" \
                    f" FROM topics AS t LEFT JOIN student_performance_summary" \
                    f" AS sps ON t.id=sps.topic_id LEFT JOIN skills AS sk ON sk.skill_id=sps.skill_id WHERE t.chapter_id={chapter_id} " \
                    f" AND t.class_id={class_exam_id} GROUP BY id"
            topics = await conn.execute_query_dict(query)
            if not topics:
                return JSONResponse(status_code=400,content={"response":"no performance summary for the user","success":False})
            topics_df = pd.DataFrame(topics)
            filt = topics_df['student_id'] != student_id
            topics_df.loc[filt, 'topic_score'] = 0
            topics_df.loc[filt, "A"] = 0
            topics_df.loc[filt, "E"] = 0
            topics_df.loc[filt, "C"] = 0
            topics_df.loc[filt, "K"] = 0
            topics_df.drop('student_id', axis=1, inplace=True)
            # print(result_df)

            total = topics_df['A'].sum() + topics_df['E'].sum() + topics_df['C'].sum() + topics_df['K'].sum()
            if total == 0:
                total = 1
            topics_df['A'] = ((topics_df['A'] / total) * 100).astype(float).round(2)

            topics_df['E'] = ((topics_df['E'] / total) * 100).astype(float).round(2)
            topics_df['C'] = ((topics_df['C'] / total) * 100).astype(float).round(2)
            topics_df['K'] = ((topics_df['K'] / total) * 100).astype(float).round(2)
            topics_df['topic_score'] = topics_df['topic_score'].astype(float).round(2)
            res=topics_df.to_dict('records')
            chapter_cache = {
                'topics': res
            }
            r.setex(str(chapter_id) + "_chapter_id", timedelta(days=1), json.dumps(chapter_cache))
            print(f"execution time : {datetime.now() - start_time}")
            return JSONResponse(status_code=200,content={'message': "Topic list by Chapter Id", 'response': res, "success": True})

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
        '''
        if r.exists(str(subject_id)+"_subject_id"):
            subject_cache=json.loads(r.get(str(subject_id)+"_subject_id"))
            if "chapters" in subject_cache:
                chapters=subject_cache['chapters']
            else:
                query = f"SELECT esc.chapter_id,esc.chapter_name,IFNULL((sps.ques_ans_correctly/sps.question_attempted)*100,0) AS score FROM exam_subject_chapters AS esc LEFT JOIN " \
                        f"student_performance_summary AS sps ON esc.chapter_id=sps.chapter_id WHERE esc.subject_id={subject_id} AND esc.class_exam_id={class_exam_id} AND sps.student_id={student_id} GROUP BY chapter_id"
                chapters = await conn.execute_query_dict(query)
                if not chapters:
                    return JSONResponse(status_code=400, content={"msg": "no Chapters for given subject or student","sucess":False})
                chapters_df = pd.DataFrame(chapters)
                chapters_df['score'] = chapters_df['score'].astype(float)
                chapters = chapters_df.to_dict('records')
                subject_cache = {
                    'chapters': chapters
                }
                r.setex(str(subject_id) + "_subject_id", timedelta(days=1), json.dumps(subject_cache))
        else:
            query = f"SELECT esc.chapter_id,esc.chapter_name,IFNULL((sps.ques_ans_correctly/sps.question_attempted)*100,0) AS score FROM exam_subject_chapters AS esc LEFT JOIN " \
                    f"student_performance_summary AS sps ON esc.chapter_id=sps.chapter_id WHERE esc.subject_id={subject_id} AND esc.class_exam_id={class_exam_id} AND sps.student_id={student_id} GROUP BY chapter_id"
            chapters = await conn.execute_query_dict(query)
            if not chapters:
                return JSONResponse(status_code=400, content={"msg": "no Chapters for given subject or student","sucess":False})
            chapters_df = pd.DataFrame(chapters)
            chapters_df['score'] = chapters_df['score'].astype(float)
            chapters = chapters_df.to_dict('records')
            subject_cache = {
                'chapters': chapters
            }
            r.setex(str(subject_id) + "_subject_id", timedelta(days=1), json.dumps(subject_cache))
        '''

        query=f"SELECT esc.chapter_id,esc.chapter_name,sps.student_id,IFNULL((sps.ques_ans_correctly/sps.question_attempted)*100,0)" \
              f" AS chapter_score,IFNULL(SUM(sk.skill_name='Evaluation'),0) AS E," \
              f" IFNULL(SUM(sk.skill_name='Comprehension'),0) AS C," \
              f" IFNULL(SUM(sk.skill_name='Application'),0) AS A," \
              f"IFNULL(SUM(sk.skill_name='Knowledge'),0) AS K FROM exam_subject_chapters " \
              f" AS esc LEFT JOIN student_performance_summary  AS sps ON esc.chapter_id=sps.chapter_id LEFT JOIN skills AS sk ON sk.skill_id=sps.skill_id" \
              f" WHERE esc.subject_id={subject_id} AND esc.class_exam_id={class_exam_id} GROUP BY chapter_id"
        result=await conn.execute_query_dict(query)

        if not result:
            return JSONResponse(status_code=400,content={"response":"no performance summary for user","success":False})
        result_df=pd.DataFrame(result)
        filt=result_df['student_id']!=student_id
        result_df.loc[filt,'chapter_score']=0
        result_df.loc[filt, "A"] = 0
        result_df.loc[filt, "E"] = 0
        result_df.loc[filt, "C"] = 0
        result_df.loc[filt, "K"] = 0
        result_df.drop('student_id',axis=1,inplace=True)
        #print(result_df)
        total=result_df['A'].sum()+result_df['E'].sum()+result_df['C'].sum()+result_df['K'].sum()
        if total==0:
            total=1
        result_df['A']=((result_df['A']/total)*100).astype(float).round(2)

        result_df['E'] = ((result_df['E'] / total) * 100).astype(float).round(2)
        result_df['C'] = ((result_df['C'] / total) * 100).astype(float).round(2)
        result_df['K'] = ((result_df['K'] / total) * 100).astype(float).round(2)
        result_df['chapter_score']=result_df['chapter_score'].astype(float).round(2)
        print(f"execution time: {datetime.now()-start_time}")
        return JSONResponse(status_code=200, content={'message': "Chapters list by subject Id", 'response': result_df.to_dict('records'),"success":True})
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