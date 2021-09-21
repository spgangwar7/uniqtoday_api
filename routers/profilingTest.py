from datetime import timedelta
from http import HTTPStatus
from typing import List
import pandas as pd
import redis
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from collections import defaultdict
import redis
import json
from datetime import datetime
router = APIRouter(
    prefix='/api',
    tags=['ProfilingTest'],
)

@router.get('/profiling-test-mobile/{exam_id}',description="New API",status_code=201)
async def StudentProfilingTest(exam_id:int=0):
    start_time=datetime.now()
    conn = Tortoise.get_connection('default')
    r = redis.Redis()
    exam_cache = {}
    response={}
    question_bank_name = ""
    if r.exists(str(exam_id) + "_profilingTestMobile"):
        print("Getting profiling test questions from redis ")
        response=json.loads(r.get(str(exam_id) + "_profilingTestMobile"))
    else:
        print("Saving profiling test questions in redis")
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
        """
        test_exam_Query=f'SELECT subject_id,questions_cnt,time_in_min FROM test_pattern_setup where class_id={exam_id} and test_name="Profiling"'
        test_exam=await conn.execute_query_dict(test_exam_Query)
        print(test_exam)
        for subject in test_exam:
            print(subject['subject_id'])
        """
        query1 = f'select a.question_id, a.class_id,  a.subject_id,a.chapter_id , a.topic_id,' \
                 f'a.question, a.template_type, a.difficulty_level, a.language_id, a.marks, a.negative_marking, a.question_options, a.answers,' \
                 f'a.time_allowed,a.passage_inst_ind, a.passage_inst_id, b.passage_inst, b.pass_inst_type ' \
                 f'FROM {question_bank_name} a left join question_bank_passage_inst b ON a.passage_inst_id = b.id ' \
                 f'where a.question_id in ' \
                 f'(select question_id from student_profiling_questions where exam_id = {exam_id}) ORDER BY RAND() limit 75'

        summary1 = await conn.execute_query_dict(query1)
        summary1 = pd.DataFrame(summary1)
        if summary1.empty:
            return JSONResponse(status_code=400, content={"response": "insufficent data or something wrong","success": False})
        subject_id_list = summary1['subject_id'].unique()

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

        query2 = f'SELECT prof_test_time_in_min FROM student_config_master WHERE exam_id = {exam_id}'
        time_allowed = await conn.execute_query_dict(query2)
        time_allowed = time_allowed[0]['prof_test_time_in_min']
        response={"time_allowed": int(time_allowed), "Subjects": subject_list, "questions_list": grouped,
                                 "success": True}
        r.setex(str(exam_id) + "_profilingTestMobile", timedelta(days=1), json.dumps(response))
    print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))
    return JSONResponse(status_code=200,
                        content=response)


@router.get('/profiling-test-web/{exam_id}',description="New API",status_code=201)
async def StudentProfilingTestWeb(exam_id:int=0):
    start_time=datetime.now()
    r = redis.Redis()
    exam_cache = {}
    question_bank_name = ""
    response={}
    if r.exists(str(exam_id)+ "_profilingTestWeb"):
        print("Getting profiling test questions from redis ")
        response=json.loads(r.get(str(exam_id) + "_profilingTestWeb"))
    else:
        conn = Tortoise.get_connection('default')
        if r.exists(str(exam_id) + "_examid"):
            exam_cache = json.loads(r.get(str(exam_id) + "_examid"))
            if "question_bank_name" in exam_cache:
                question_bank_name = exam_cache['question_bank_name']
            else:
                query = f'select question_bank_name from question_bank_tables where exam_id={exam_id}'
                summ = await conn.execute_query_dict(query)
                question_bank_name = summ[0]['question_bank_name']
                exam_cache['question_bank_name'] = question_bank_name
                r.setex(str(exam_id) + "_examid",timedelta(days=1), json.dumps(exam_cache))
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
                 f'(select question_id from student_profiling_questions where exam_id = {exam_id}) ORDER BY RAND() limit 75'

        summary1 = await conn.execute_query_dict(query1)
        summary1 = pd.DataFrame(summary1)
        if summary1.empty:
            return JSONResponse(status_code=400, content={"response": "insufficent data or something wrong","success": False})
        subject_id_list = summary1['subject_id'].unique()

        if len(subject_id_list) > 1:
            subject_id_list = tuple(subject_id_list)
        elif len(subject_id_list) == 1:
            subject_id_list = subject_id_list[0]
            subject_id_list = "(" + str(subject_id_list) + ")"
        summary1 = summary1.fillna(0)
        summary1 = summary1.to_dict('records')

        # Subject List by exam ID
        query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} and subject_id in {subject_id_list} group by exam_subjects.subject_id'
        subject_list = await conn.execute_query_dict(query)

        query2 = f'SELECT prof_test_time_in_min FROM student_config_master WHERE exam_id = {exam_id}'
        time_allowed = await conn.execute_query_dict(query2)
        time_allowed = time_allowed[0]['prof_test_time_in_min']
        response = {"time_allowed": int(time_allowed), "Subjects": subject_list, "questions_list": summary1,
                    "success": True}
        print("Saving profiling test questions in redis")
        r.setex(str(exam_id) + "_profilingTestWeb",timedelta(days=1),json.dumps(response))
    print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))

    return JSONResponse(status_code=200,
                        content=response)

