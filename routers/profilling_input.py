import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from collections import defaultdict

router = APIRouter(
    prefix='/api',
    tags=['profilling_input'],
)



@router.get('/profiling-input/{exam_id}/{count}',description="New API",status_code=201)
async def student_profiling_input(exam_id:int=0,count:int=0):
    try:
        conn = Tortoise.get_connection('default')
        try:
            query = f'select question_bank_name from question_bank_tables where exam_id={exam_id}'
            summ = await conn.execute_query_dict(query)
            que_bank = summ[0]['question_bank_name']
        except:
            print("invalid exam id")
        print(que_bank)
        query1 = f'select a.question_id, a.class_id,  a.subject_id,a.chapter_id , a.topic_id,' \
                 f'a.question, a.template_type, a.difficulty_level, a.language_id, a.marks, a.negative_marking, a.question_options, a.answers,' \
                 f'a.time_allowed,   a.passage_inst_ind, a.passage_inst_id, b.passage_inst, b.pass_inst_type ' \
                 f'FROM {que_bank} a left join question_bank_passage_inst b ON a.passage_inst_id = b.id ' \
                 f' where a.question_id in ' \
                 f'(select question_id from student_profiling_questions where exam_id = {exam_id}) ORDER BY RAND() limit {count} '

        sql_time = f'SELECT prof_test_time_in_min FROM student_config_master WHERE exam_id = {exam_id}'
        timeAllowedDf = await conn.execute_query_dict(sql_time)
        time_allowed = timeAllowedDf[0]['prof_test_time_in_min']
        summary1 = await conn.execute_query_dict(query1)
        summary1=pd.DataFrame(summary1)
        subject_id_list = summary1['subject_id'].unique()
        print(len(subject_id_list))
        if len(subject_id_list)>1:
            subject_id_list = tuple(subject_id_list)
        elif  len(subject_id_list)==1:
            subject_id_list=subject_id_list[0]
            subject_id_list="("+str(subject_id_list)+")"
        summary1=summary1.fillna(0)
        summary1=summary1.to_dict('records')
        #restructure dict on subject_id
        grouped = {}
        for dict in summary1:
            grouped.setdefault(dict['subject_id'], []).append(
                {k: v for k, v in dict.items() if k != 'subject_id'})
        grouped = [{'subject_id': k, 'Questions': v} for k, v in grouped.items()]

        #Subject List by exam ID
        query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} and subject_id in {subject_id_list} group by exam_subjects.subject_id'
        subject_list = await conn.execute_query_dict(query)
        return JSONResponse(status_code=200,
                            content={"time_allowed": int(time_allowed),"Subjects":subject_list, "questions_list": grouped,
                                     "success": True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})



@router.get('/profiling-input2/{exam_id}/{count}',description="Old API",status_code=201)
async def student_profiling_input2(exam_id:int=0,count:int=0):
    try:
        conn = Tortoise.get_connection('default')
        try:
            query = f'select question_bank_name from question_bank_tables where exam_id={exam_id}'
            summ = await conn.execute_query_dict(query)
            que_bank = summ[0]['question_bank_name']
        except:
            print("invalid exam id")
        print(que_bank)
        query1 = f'select a.question_id, a.class_id,  a.subject_id,a.chapter_id, a.topic_id,' \
                 f'a.question, a.template_type, a.difficulty_level, a.language_id, a.marks, a.negative_marking, a.question_options, a.answers,' \
                 f'a.time_allowed,   a.passage_inst_ind, a.passage_inst_id, b.passage_inst, b.pass_inst_type ' \
                 f'FROM {que_bank} a left join question_bank_passage_inst b ON a.passage_inst_id = b.id ' \
                 f' where a.question_id in ' \
                 f'(select question_id from student_profiling_questions where exam_id = {exam_id}) limit {count}'

        sql_time = f'SELECT prof_test_time_in_min FROM student_config_master WHERE exam_id = {exam_id}'
        timeAllowedDf = await conn.execute_query_dict(sql_time)
        time_allowed = timeAllowedDf[0]['prof_test_time_in_min']
        summary1 = await conn.execute_query_dict(query1)
        summary1=pd.DataFrame(summary1)
        summary1=summary1.fillna(0)
        summary1=summary1.to_dict('records')
        return JSONResponse(status_code=200,
                            content={"time_allowed": int(time_allowed), "Success": "True", "questions": summary1,
                                     "success": True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})