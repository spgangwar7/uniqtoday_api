import json
import traceback
from datetime import datetime, timedelta
from http import HTTPStatus
import pandas as pd
import redis
from fastapi import APIRouter
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
router = APIRouter(
    prefix='/api/live-exam',
    tags=['Live Exams'],
)
def td_to_str(td):
    """
    convert a timedelta object td to a string in HH:MM:SS format.
    """
    hours, remainder = divmod(td.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{int(hours):02}:{int(minutes):02}:{int(seconds):02}'

@router.get('/get-all', description='Get All Live Exams', status_code=201)
async def getAllLiveExams():
    try:
        conn = Tortoise.get_connection("default")
        query = 'select * from ct_exams_list where grade_id = 1 and exam_type="Live" and exam_date >= curdate()'
        val = await conn.execute_query_dict(query)
        val=pd.DataFrame(val)
        print(val)
        val['start_time'] = val['start_time'].apply(td_to_str)
        val['end_time'] = val['end_time'].fillna(pd.Timedelta(0))
        val['end_time'] = val['end_time'].apply(td_to_str)
        val['exam_date'] = val['exam_date'].astype(str)
        val['result_date'] = val['result_date'].astype(str)
        val['createdAt'] = val['createdAt'].dt.strftime('%Y-%m-%d')
        val['updatedAt'] = val['updatedAt'].dt.strftime('%Y-%m-%d')
        val=val.fillna(0)
        resp={
            'message': "Live Exam List",
            'response': val.to_dict(orient='records'),
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})

@router.get('/live-exam-schedule/{exam_id}',description='get all exams schedules for today or in future')
async def liveExamSchedule(exam_id:int=0):
    conn=Tortoise.get_connection('default')
    today_date=datetime.today().strftime('%y-%m-%d')
    query=f"SELECT es.id as schedule_id,es.class_exam_id as exam_id,ce.class_exam_cd as exam_name,es.exam_type,es.subject_id,s.subject_name,es.chapter_id,es.unit_id," \
          f"es.topic_id,es.skill_id,DATE_FORMAT(es.start_date,'%d-%m-%y') as start_date," \
          f"DATE_FORMAT(es.end_date,'%d-%m-%y') as end_date,DATE_FORMAT(es.result_date,'%d-%m-%y') as result_date," \
          f"es.questions_count,es.STATUS FROM exam_schedule es " \
          f"inner join class_exams as ce on es.class_exam_id=ce.id " \
          f"inner join subjects s on es.subject_id=s.id" \
          f" WHERE start_date >= '{today_date}' and es.exam_type='Live' and es.class_exam_id={exam_id}"
    res=await conn.execute_query_dict(query)
    if not res:
        return JSONResponse(status_code=400,content={'response':f"no live exam scheduled","success":False})
    return JSONResponse(status_code=200,content={'response':res,"success":True})

@router.get('/live-exam-mobile/{exam_id}',description="Live Exam",status_code=201)
async def LiveExamTest(exam_id:int=0):
    start_time=datetime.now()
    conn = Tortoise.get_connection('default')
    r = redis.Redis()
    exam_cache = {}
    response={}
    question_bank_name = ""
    if r.exists(str(exam_id) + "_liveExamMobile"):
        print("Getting live exam  questions from redis ")
        response=json.loads(r.get(str(exam_id) + "_profilingTestMobile"))
    else:
        print("Saving live exam questions in redis")
        if r.exists(str(exam_id) + "_examid"):
            exam_cache = json.loads(r.get(str(exam_id) + "_examid"))
            if "question_bank_name" in exam_cache:
                question_bank_name = exam_cache['question_bank_name']
            else:
                query = f'select question_bank_name from question_bank_tables where exam_id={exam_id}'
                summ = await conn.execute_query_dict(query)
                question_bank_name = summ[0]['question_bank_name']
                exam_cache['question_bank_name'] = question_bank_name
                r.setex(str(exam_id) + "_examid", timedelta(days=1),json.dumps(exam_cache))
        else:
            query = f'select question_bank_name from question_bank_tables where exam_id={exam_id}'
            summ = await conn.execute_query_dict(query)
            question_bank_name = summ[0]['question_bank_name']
            exam_cache['question_bank_name'] = question_bank_name
            r.setex(str(exam_id) + "_examid", timedelta(days=1),json.dumps(exam_cache))

        query1 = f'select a.question_id, a.class_id,  a.subject_id,a.chapter_id , a.topic_id,' \
                 f'a.question, a.template_type, a.difficulty_level, a.language_id, a.marks, a.negative_marking, a.question_options, a.answers,' \
                 f'a.time_allowed,a.passage_inst_ind, a.passage_inst_id, b.passage_inst, b.pass_inst_type ' \
                 f'FROM {question_bank_name} a left join question_bank_passage_inst b ON a.passage_inst_id = b.id ' \
                 f'where a.question_id in ' \
                 f'(select question_id from student_live_questions_set where exam_id = {exam_id}) ORDER BY RAND() limit 75'

        summary1 = await conn.execute_query_dict(query1)
        summary1 = pd.DataFrame(summary1)
        if summary1.empty:
            return JSONResponse(status_code=400, content={"response": "insufficent data or something wrong","success": False})
        subject_id_list = summary1['subject_id'].unique()
        total_questions=len(summary1.index)
        if len(subject_id_list) > 1:
            subject_id_list = tuple(subject_id_list)
        elif len(subject_id_list) == 1:
            subject_id_list = subject_id_list[0]
            subject_id_list = "(" + str(subject_id_list) + ")"
        summary1 = summary1.fillna(0)
        summary1 = summary1.to_dict('records')
        # restructure dict on subject_id
        grouped = {}
        for dict in summary1:
            grouped.setdefault(dict['subject_id'], []).append(
                {k: v for k, v in dict.items() if k != 'subject_id'})
        grouped = [{'subject_id': k, 'Questions': v} for k, v in grouped.items()]

        # Subject List by exam ID
        query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} and subject_id in {subject_id_list} group by exam_subjects.subject_id'
        subject_list = await conn.execute_query_dict(query)

        response={"time_allowed": total_questions, "Subjects": subject_list, "questions_list": grouped,
                                 "success": True}
        #r.setex(str(exam_id) + "_profilingTestMobile", timedelta(days=1), json.dumps(response))
    print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))
    return JSONResponse(status_code=200,
                        content=response)


@router.get('/live-exam-web/{schedule_id}',description="Live Exam",status_code=201)
async def LiveExamTest(schedule_id:int=0):
    start_time=datetime.now()
    conn = Tortoise.get_connection('default')
    query=f'select class_exam_id as exam_id,question_ids from exam_schedule where id={schedule_id}'
    res=await conn.execute_query_dict(query)
    if not res:
        return JSONResponse(status_code=400,content={'response':f'no live exam for this schedule_id:{schedule_id}',"success":False})
    exam_id=res[0]['exam_id']
    question_ids=json.loads(res[0]['question_ids'])
    question_ids=list(map(int,question_ids))
    print(question_ids)
    r = redis.Redis()
    exam_cache = {}
    response={}

    question_bank_name = ""
    if r.exists(str(schedule_id) + "_liveExamMobile"):
        print("Getting live exam  questions from redis ")
        response=json.loads(r.get(str(schedule_id) + "_liveExamMobile"))
    else:
        print("Saving live exam questions in redis")
        if r.exists(str(exam_id) + "_examid"):
            exam_cache = json.loads(r.get(str(exam_id) + "_examid"))
            if "question_bank_name" in exam_cache:
                question_bank_name = exam_cache['question_bank_name']
            else:
                query = f'select question_bank_name from question_bank_tables where exam_id={exam_id}'
                summ = await conn.execute_query_dict(query)
                question_bank_name = summ[0]['question_bank_name']
                exam_cache['question_bank_name'] = question_bank_name
                r.setex(str(exam_id) + "_examid", timedelta(days=1),json.dumps(exam_cache))
        else:
            query = f'select question_bank_name from question_bank_tables where exam_id={exam_id}'
            summ = await conn.execute_query_dict(query)
            question_bank_name = summ[0]['question_bank_name']
            exam_cache['question_bank_name'] = question_bank_name
            r.setex(str(exam_id) + "_examid", timedelta(days=1),json.dumps(exam_cache))

        query1 = f'select a.question_id, a.class_id,  a.subject_id,a.chapter_id , a.topic_id,' \
                 f'a.question, a.template_type, a.difficulty_level, a.language_id, a.marks, a.negative_marking, a.question_options, a.answers,' \
                 f'a.time_allowed,a.passage_inst_ind, a.passage_inst_id, b.passage_inst, b.pass_inst_type ' \
                 f'FROM {question_bank_name} a left join question_bank_passage_inst b ON a.passage_inst_id = b.id ' \
                 f'where a.question_id in {tuple(question_ids)}'

        summary1 = await conn.execute_query_dict(query1)
        summary1 = pd.DataFrame(summary1)
        if summary1.empty:
            return JSONResponse(status_code=400, content={"response": "insufficent data or something wrong","success": False})
        subject_id_list = summary1['subject_id'].unique()
        total_questions=len(summary1.index)
        if len(subject_id_list) > 1:
            subject_id_list = tuple(subject_id_list)
        elif len(subject_id_list) == 1:
            subject_id_list = subject_id_list[0]
            subject_id_list = "(" + str(subject_id_list) + ")"
        summary1 = summary1.fillna(0)
        summary1 = summary1.to_dict('records')
        # restructure dict on subject_id

        # Subject List by exam ID
        query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} and subject_id in {subject_id_list} group by exam_subjects.subject_id'
        subject_list = await conn.execute_query_dict(query)

        response={"time_allowed": total_questions, "Subjects": subject_list, "questions_list": summary1,
                                 "success": True}
        r.setex(str(schedule_id) + "_liveExamMobile", timedelta(days=1), json.dumps(response))
    print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))
    return JSONResponse(status_code=200,
                        content=response)