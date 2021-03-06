import asyncio
import json
import traceback
import pandas as pd
import redis
from celery import Celery
import os
from fastapi import APIRouter
from starlette.responses import JSONResponse
from tortoise import Tortoise
from datetime import datetime,date,timedelta
from asgiref.sync import async_to_sync
import MySQLdb

from tortoise.contrib.fastapi import register_tortoise
os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')
celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://")


from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

db_url = os.environ.get("DB_URL")
db_host = os.environ.get("DB_HOST")
db_user = os.environ.get("DB_USER")
db_pass = os.environ.get("DB_PASS")
db_port = os.environ.get("DB_PORT")
db_name = os.environ.get("DB_NAME")


def db_connection():
    connection = MySQLdb.connect(host=db_host,
    user=db_user,
    password=db_pass,charset='utf8',port=db_port) # create the connection
    cursor = connection.cursor() # get the cursor
    query=f'use {db_name}'
    cursor.execute(query) # select the DB
    return connection,cursor


@celery.task(name="add_task")
def add_task(x, y):
    return x+y

router = APIRouter(
    prefix='/api',
    tags=['Queues'],
)


def save_student_summary(student_id:int,exam_id:int):
    try:
        start_time=datetime.now()

        #conn = Tortoise.get_connection("default")
        conn,crsr= db_connection()

        #Initializing Redis
        r = redis.Redis()
        exam_cache={}
        classTablename=""
        if r.exists(str(exam_id) + "_examid"):
            exam_cache = json.loads(r.get(str(exam_id) + "_examid"))
            if "question_bank_name" in exam_cache:
                classTablename = exam_cache['question_bank_name']
            else:
                query_class_exam_data = f"SELECT question_bank_name FROM class_exams WHERE id = {exam_id}"
                class_exam_data = pd.read_sql(query_class_exam_data,con=conn)
                classTablename = class_exam_data['question_bank_name'].iloc[0]
                exam_cache={
                    'question_bank_name':classTablename
                }
                r.setex(str(exam_id) + "_examid",timedelta(days=1),json.dumps(exam_cache))
        else:
            query_class_exam_data = f"SELECT question_bank_name FROM class_exams WHERE id = {exam_id}"
            class_exam_data = pd.read_sql(query_class_exam_data, con=conn)
            classTablename = class_exam_data['question_bank_name'].iloc[0]
            exam_cache = {
                'question_bank_name': classTablename
            }
            r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))
        result2=[]

        query = f'SELECT id,class_exam_id,student_id,student_result_id,sqa.subject_id,sqa.chapter_id,sqa.topic_id,exam_type,' \
                f'sqa.question_id,attempt_status,sqa.gain_marks,unit_id,skill_id,difficulty_level,major_concept_id,sqa.created_on FROM ' \
                f'student_questions_attempted as sqa left join {classTablename} as qbj on sqa.question_id=qbj.question_id ' \
                f'where student_id={student_id} and class_exam_id={exam_id}'
        resultdf= pd.read_sql(query,con=conn)
        resultdf["created_on"]=pd.to_datetime(resultdf["created_on"]).dt.strftime('%Y-%m-%d')
        if resultdf.empty:
            return JSONResponse(status_code=400,content={"response":"invalid credentials","success":False})

        resultdf=resultdf.fillna(0)

        dfgrouponehot = pd.get_dummies(resultdf, columns=['attempt_status'], prefix=['attempt_status'])
        #dfgrouponehot = dfgrouponehot.fillna(0)
        if 'attempt_status_Correct' not in dfgrouponehot:
            dfgrouponehot['attempt_status_Correct'] = 0
        if 'attempt_status_Incorrect' not in dfgrouponehot:
            dfgrouponehot['attempt_status_Incorrect'] = 0
        if 'attempt_status_Unanswered' not in dfgrouponehot:
            dfgrouponehot['attempt_status_Unanswered'] = 0
        #print(dfgrouponehot.isnull().sum(axis = 0))
        pivotdf = dfgrouponehot.pivot_table(
            values=['attempt_status_Correct', 'attempt_status_Incorrect', 'attempt_status_Unanswered'],
            index=['student_id', 'class_exam_id', 'subject_id', 'unit_id', 'chapter_id', 'topic_id', 'skill_id',
                   'difficulty_level', 'major_concept_id', 'created_on', 'gain_marks'],
            columns=[],
            aggfunc='sum')
        del_question(student_id)
        newdf = pd.DataFrame(pivotdf.to_records())
        print("Inserting in student performance summary")
        newdf=newdf.to_dict('records')
        for dict in newdf:
            student_id=dict['student_id']
            class_exam_id=dict['class_exam_id']
            subject_id=dict['subject_id']
            unit_id=dict['unit_id']
            chapter_id=dict['chapter_id']
            topic_id=dict['topic_id']
            skill_id=dict['skill_id']
            difficulty_level=dict['difficulty_level']
            major_concept_id=dict['major_concept_id']
            created_on=dict['created_on']
            gain_marks=dict['gain_marks']
            attempt_status_Correct=dict['attempt_status_Correct']
            attempt_status_Incorrect=dict['attempt_status_Incorrect']
            attempt_status_Unanswered=dict['attempt_status_Unanswered']
            quesattempted=int(attempt_status_Correct)+int(attempt_status_Incorrect)+int(attempt_status_Unanswered)
            query_insert = f'INSERT INTO student_performance_summary (student_id, exam_id, subject_id, unit_id, chapter_id, \
                        topic_id, skill_id, ques_difficulty_level, major_concept_id,last_test_date,question_attempted, ques_ans_correctly, \
                        ques_ans_incorrectly, ques_unattempted_cnt,marks) VALUES ({student_id},{class_exam_id},{subject_id},{unit_id}, \
                        {chapter_id},{topic_id},{skill_id},{difficulty_level},{major_concept_id},"{created_on}",{quesattempted},{attempt_status_Correct}, \
                        {attempt_status_Incorrect},{attempt_status_Unanswered},{gain_marks})'
            crsr.execute(query_insert)


        resp={"response":"Records inserted in db successfully"
            ,"success":True}
        print(f"execution time is {(datetime.now()-start_time)}")
        #await Tortoise.close_connections()

        return resp
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return {"error":f"{e}","success":False}
    finally:
        crsr.close()
        conn.close()


def del_question(student_id):
    try:
        #conn = Tortoise.get_connection("default")
        conn,crsr= db_connection()
        del_query = f'DELETE FROM student_performance_summary WHERE student_id={student_id}'
        crsr.execute(del_query)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)


@router.get('/save_student_summary', status_code=201)
@celery.task(name="save_student_summary")
def save_summary_task(student_id:int,exam_id:int):
    task=save_student_summary(student_id,exam_id)
    #task=asyncio.run(save_student_summary(student_id,exam_id))
    return task

@router.get('/celery-test', status_code=201)
async def celery_test(user_id:int,exam_id:int):
    task = save_summary_task.delay(user_id,exam_id)
    return JSONResponse({"task_id": task.id})

@router.get("/tasks/{task_id}")
def get_status(task_id):
    task_result = celery.AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return result