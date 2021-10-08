import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
import numpy as np
import redis
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import IntegrityError
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from tortoise.models import Model
from tortoise.transactions import in_transaction
import json
from datetime import datetime,timedelta

router = APIRouter(
    prefix='/api',
    tags=['Question Reviews'],
)


@router.get('/question-reviews/{result_id}', description='get question reviews')
async def questionReviews(result_id: int = 0):
    start_time=datetime.now()
    conn = Tortoise.get_connection("default")
    r = redis.Redis()
    result_cache = {}
    user_id=0
    exam_id=0
    if r.exists(str(result_id) + "_result_id"):
        result_cache = json.loads(r.get(str(result_id) + "_result_id"))
        if "user_id" and "exam_id" in result_cache:
            user_id = result_cache['user_id']
            exam_id = result_cache['exam_id']
        else:
            query = f'select user_id,class_grade_id as class_id from user_result where id ={result_id}'
            result = await conn.execute_query_dict(query)
            user_id = result[0]['user_id']
            exam_id = result[0]['class_id']
            result_cache['user_id'] = user_id
            result_cache['exam_id'] = exam_id
            r.setex(str(result_id) + "_result_id", timedelta(days=1), json.dumps(result_cache))
    else:
        query = f'select user_id,class_grade_id as class_id from user_result where id ={result_id}'
        result = await conn.execute_query_dict(query)
        user_id = result[0]['user_id']
        exam_id = result[0]['class_id']
        result_cache['user_id'] = user_id
        result_cache['exam_id'] = exam_id
        r.setex(str(result_id) + "_result_id", timedelta(days=1), json.dumps(result_cache))

    exam_cache={}
    question_bank_name=''
    if r.exists(str(exam_id)+"_examid"):
        exam_cache=json.loads(r.get(str(exam_id)+"_examid"))
        if "question_bank_name" in exam_cache:
            question_bank_name=exam_cache['question_bank_name']
        else:
            query1 = f'select question_bank_name from class_exams where id = {exam_id}'
            qbanktable = await conn.execute_query_dict(query1)
            question_bank_name = qbanktable[0]['question_bank_name']
            exam_cache['question_bank_name']=question_bank_name
            r.setex(str(exam_id)+"_examid",timedelta(days=1),json.dumps(exam_cache))
    else:
        query1 = f'select question_bank_name from class_exams where id = {exam_id}'
        qbanktable = await conn.execute_query_dict(query1)
        question_bank_name = qbanktable[0]['question_bank_name']
        exam_cache['question_bank_name'] = question_bank_name
        r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))

    query3 = f'select distinct question_id from student_questions_attempted where student_result_id = {result_id}'
    question_idlist = await conn.execute_query_dict(query3)
    if question_idlist:
        question_idframe = pd.DataFrame(question_idlist)
        questionids_string1 = question_idframe['question_id'].values.tolist()
        first = question_idframe['question_id'].iloc[0]
        last = question_idframe['question_id'].iloc[-1]
        question_idframe = question_idframe.loc[:, 'question_id']
        questionids = question_idframe.values
        questionids_string = ", ".join(map(str, questionids))
        # print(questionids)
        query4 = f'select qtable.question_id,qtable.subject_id,qtable.chapter_id,question,tags,qtable.difficulty_level,template_type,skill_id, \
                                 atm.attempt_status,atm.option_id,language_id,explanation, \
                                 question_options,answers,reference_text from {question_bank_name} as qtable left join  student_questions_attempted \
                                 as atm on atm.question_id = qtable.question_id where qtable.question_id IN ({questionids_string}) and student_result_id = {result_id}'
        final_result = await conn.execute_query_dict(query4)
        resp = {"question_ids": questionids_string1, "all_question": final_result, "first": str(first),
                "last": str(last), "success": True}
    else:
        resp = {"message": "Data does not exist for this result id", "sucess": False}
        return JSONResponse(status_code=400, content=resp)
    endtime=datetime.now()
    print(endtime-start_time)
    return JSONResponse(status_code=200, content=resp)


@router.get('/question-reviews-mobile/{result_id}', description='get question reviews')
async def questionReviews(result_id: int = 0):
    start_time=datetime.now()
    conn = Tortoise.get_connection("default")
    r = redis.Redis()
    result_cache = {}
    user_id=0
    exam_id=0
    if r.exists(str(result_id) + "_result_id"):
        result_cache = json.loads(r.get(str(result_id) + "_result_id"))
        if "user_id" and "exam_id" in result_cache:
            user_id = result_cache['user_id']
            exam_id = result_cache['exam_id']
        else:
            query = f'select user_id,class_grade_id as class_id from user_result where id ={result_id}'
            result = await conn.execute_query_dict(query)
            user_id = result[0]['user_id']
            exam_id = result[0]['class_id']
            result_cache['user_id'] = user_id
            result_cache['exam_id'] = exam_id
            r.setex(str(result_id) + "_result_id", timedelta(days=1), json.dumps(result_cache))
    else:
        query = f'select user_id,class_grade_id as class_id from user_result where id ={result_id}'
        result = await conn.execute_query_dict(query)
        user_id = result[0]['user_id']
        exam_id = result[0]['class_id']
        result_cache['user_id'] = user_id
        result_cache['exam_id'] = exam_id
        r.setex(str(result_id) + "_result_id", timedelta(days=1), json.dumps(result_cache))

    exam_cache={}
    question_bank_name=''
    if r.exists(str(exam_id)+"_examid"):
        exam_cache=json.loads(r.get(str(exam_id)+"_examid"))
        if "question_bank_name" in exam_cache:
            question_bank_name=exam_cache['question_bank_name']
        else:
            query1 = f'select question_bank_name from class_exams where id = {exam_id}'
            qbanktable = await conn.execute_query_dict(query1)
            question_bank_name = qbanktable[0]['question_bank_name']
            exam_cache['question_bank_name']=question_bank_name
            r.setex(str(exam_id)+"_examid",timedelta(days=1),json.dumps(exam_cache))
    else:
        query1 = f'select question_bank_name from class_exams where id = {exam_id}'
        qbanktable = await conn.execute_query_dict(query1)
        question_bank_name = qbanktable[0]['question_bank_name']
        exam_cache['question_bank_name'] = question_bank_name
        r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))

    query3 = f'select distinct question_id from student_questions_attempted where student_result_id = {result_id}'
    question_idlist = await conn.execute_query_dict(query3)
    if question_idlist:
        question_idframe = pd.DataFrame(question_idlist)
        questionids_string1 = question_idframe['question_id'].values.tolist()
        first = question_idframe['question_id'].iloc[0]
        last = question_idframe['question_id'].iloc[-1]
        question_idframe = question_idframe.loc[:, 'question_id']
        questionids = question_idframe.values
        questionids_string = ", ".join(map(str, questionids))
        # print(questionids)
        query4 = f'select qtable.question_id,qtable.subject_id,qtable.chapter_id,question,tags,qtable.difficulty_level,template_type,skill_id, \
                                 atm.attempt_status,atm.option_id,language_id,explanation, \
                                 question_options,answers,reference_text from {question_bank_name} as qtable left join  student_questions_attempted \
                                 as atm on atm.question_id = qtable.question_id where qtable.question_id IN ({questionids_string}) and student_result_id = {result_id}'
        final_result = await conn.execute_query_dict(query4)
        summary1 = pd.DataFrame(final_result)
        summary1['answer_key'] = summary1.apply(lambda row: list(json.loads(row.answers).keys())
                                                     , axis=1)
        #print(summary1)
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


        resp = {"question_ids": questionids_string1,"Subjects": subject_list, "all_question": grouped, "first": str(first),
                "last": str(last), "success": True}
    else:
        resp = {"message": "Data does not exist for this result id", "sucess": False}
        return JSONResponse(status_code=400, content=resp)
    endtime=datetime.now()
    print(endtime-start_time)
    return JSONResponse(status_code=200, content=resp)

