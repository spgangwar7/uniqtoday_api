from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from datetime import datetime
import json
router = APIRouter(
    prefix='/api',
    tags=['testSeries'],
)



@router.get("/testSeries-questions/{exam_id}/{series_id}",description="retrieve all question in a testseries through exam_id and series_id",status_code=201)
async def testSeries_questions(exam_id:int=0,series_id:int=0):
    conn = Tortoise.get_connection("default")
    query = 'select id,question_bank_name from class_exams where id={}'.format(exam_id)
    df = await conn.execute_query_dict(query)
    df = pd.DataFrame(df)
    if df.empty:
        return JSONResponse(status_code=400,content={"msg": f"no data fro exam_id : {exam_id}","success":False})
    df.set_index('id', inplace=True)
    df = df.to_dict('index')
    question_bank_name = df[exam_id]['question_bank_name']
    query1 = 'select test_series_id,series_question_ids,time_allowed from test_series where test_series_id={}'.format(
        series_id)
    df1 = await conn.execute_query_dict(query1)
    df1=pd.DataFrame(df1)
    if df1.empty:
        return JSONResponse(status_code=400,content={"msg": f"no data for series_id : {series_id}","success":False})

    df1.set_index('test_series_id', inplace=True)
    series_data = df1.to_dict('index')
    series_question_ids = series_data[series_id]['series_question_ids']

    questiond_ids = series_question_ids.replace('[', '')
    question_ids = questiond_ids.replace(']', '')
    question_ids = question_ids.replace('"', '')
    series_question_ids = [p.strip() for p in question_ids.split(',') if p]
    series_question_ids = tuple(map(int, series_question_ids))
    print(series_question_ids)
    time_allowed = series_data[series_id]['time_allowed']
    query2 = f'select ques.question_id,ques.question,ques.template_type,ques.explanation,ques.reference_text,ques.subject_id as subt_id,' \
             f'ques.tags,ques.question_options,ques.answers,ques.passage_inst_ind,passage_inst_id,passage.passage_inst,passage.pass_inst_type,' \
             f'ques.class_id,ques.difficulty_level,ques.language_id from {question_bank_name} as ques left join question_bank_passage_inst as passage on passage.id=ques.passage_inst_id where ' \
             f'question_id in {series_question_ids}'
    data = await conn.execute_query_dict(query2)
    return JSONResponse(status_code=200,content={'message': 'Exam TestSeries Data', 'success': True, 'questions': data, 'time_allowed': time_allowed})

@router.get("/testSeries-questions-mobile/{exam_id}/{series_id}",description="retrieve all question in a testseries through exam_id and series_id",status_code=201)
async def testSeries_questions(exam_id:int=0,series_id:int=0):
    conn = Tortoise.get_connection("default")
    query = 'select id,question_bank_name from class_exams where id={}'.format(exam_id)
    df = await conn.execute_query_dict(query)
    df = pd.DataFrame(df)
    if df.empty:
        return JSONResponse(status_code=400,content={"msg": f"no data fro exam_id : {exam_id}","success":False})
    df.set_index('id', inplace=True)
    df = df.to_dict('index')
    question_bank_name = df[exam_id]['question_bank_name']
    query1 = 'select test_series_id,series_question_ids,time_allowed from test_series where test_series_id={}'.format(
        series_id)
    df1 = await conn.execute_query_dict(query1)
    df1=pd.DataFrame(df1)
    if df1.empty:
        return JSONResponse(status_code=400,content={"msg": f"no data for series_id : {series_id}","success":False})

    df1.set_index('test_series_id', inplace=True)
    series_data = df1.to_dict('index')
    series_question_ids = series_data[series_id]['series_question_ids']

    questiond_ids = series_question_ids.replace('[', '')
    question_ids = questiond_ids.replace(']', '')
    question_ids = question_ids.replace('"', '')
    series_question_ids = [p.strip() for p in question_ids.split(',') if p]
    series_question_ids = tuple(map(int, series_question_ids))
    print(series_question_ids)
    time_allowed = series_data[series_id]['time_allowed']
    subject_query=f"SELECT s.subject_name,qbt.subject_id FROM subjects AS s LEFT JOIN " \
                  f"question_bank_jee AS qbt ON s.id=qbt.subject_id WHERE qbt.question_id " \
                  f"IN {series_question_ids} group by subject_id"
    subjects=await conn.execute_query_dict(subject_query)
    query2 = f'select ques.question_id,ques.question,ques.template_type,ques.explanation,ques.reference_text,ques.subject_id as subt_id,' \
             f'ques.tags,ques.question_options,ques.answers,ques.passage_inst_ind,passage_inst_id,passage.passage_inst,passage.pass_inst_type,' \
             f'ques.class_id,ques.difficulty_level,ques.language_id from {question_bank_name} as ques left join question_bank_passage_inst as passage on passage.id=ques.passage_inst_id where ' \
             f'question_id in {series_question_ids}'
    data = await conn.execute_query_dict(query2)
    return JSONResponse(status_code=200,content={'message': 'Exam TestSeries Data',"subjects":subjects,'questions': data, 'time_allowed': time_allowed,'success': True})

@router.get("/testSeries-list/{exam_id}",description="list all testSeries through exam_id",status_code=201)
async def testSeries_list(exam_id:int):
    try:
        conn = Tortoise.get_connection("default")
        date = datetime.today().strftime('%Y-%m-%d')
        query = 'select ts.test_series_id,ts.test_series_name,ts.series_type,ts.test_series_date as series_start_date,ts.series_end_date,' \
                'ts.result_date,ts.time_allowed,ts.time_unit,ts.subscription_type,ts.questions_count from test_series as ts where series_type=1 and class_exam_id={}'.format(
            exam_id)
        open_test = await conn.execute_query_dict(query)
        open_test = pd.DataFrame(open_test)
        # open_test=open_test.to_json(orient='records')
        query1 = 'select ts.test_series_id,ts.test_series_name,ts.series_type,ts.test_series_date as series_start_date,ts.series_end_date,' \
                 'ts.result_date,ts.time_allowed,ts.time_unit,ts.subscription_type,ts.questions_count from test_series as ts where ts.series_type=2 and test_series_date <= "{}" and series_end_date >= "{}" and class_exam_id={}'.format(
            date, date, exam_id)
        test_series_live = await conn.execute_query_dict(query1)
        test_series_live = pd.DataFrame(test_series_live)

        resp = {'test_series_open': open_test.to_json(orient='records'),
                'test_series_live': test_series_live.to_json(orient='records'), "success": True}
        return JSONResponse(status_code=200, content=resp)
    except Exception as e:
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})

@router.get("/testSeries-list-mobile/{exam_id}",description="list all testSeries through exam_id",status_code=201)
async def testSeries_list(exam_id:int):
    try:
        conn = Tortoise.get_connection("default")
        today_date = datetime.today().strftime('%Y-%m-%d')
        query =f'select ts.test_series_id,ts.test_series_name,ts.series_type,DATE_FORMAT(ts.test_series_date,"%d-%m-%y") as series_start_date,DATE_FORMAT(ts.series_end_date,"%d-%m-%y") as series_end_date,' \
            f'DATE_FORMAT(ts.result_date,"%d-%m-%y") as result_date,ts.time_allowed,ts.time_unit,ts.subscription_type,ts.questions_count from test_series as ts where series_type="Open" and class_exam_id={exam_id}'
        open_test = await conn.execute_query_dict(query)
        query1 = f'select ts.test_series_id,ts.test_series_name,ts.series_type,DATE_FORMAT(ts.test_series_date,"%d-%m-%y") as series_start_date,DATE_FORMAT(ts.series_end_date,"%d-%m-%y") as series_end_date,' \
                 f'DATE_FORMAT(ts.result_date,"%d-%m-%y") as result_date,ts.time_allowed,ts.time_unit,ts.subscription_type,ts.questions_count from test_series as ts where ts.series_type="Live" and test_series_date <= "{today_date}" and series_end_date >= "{today_date}" and class_exam_id={exam_id}'
        test_series_live = await conn.execute_query_dict(query1)
        resp = {'test_series_open': open_test,
                'test_series_live': test_series_live, "success": True}
        return JSONResponse(status_code=200, content=resp)
    except Exception as e:
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})


@router.get('/testseries-report/{student_id}',description="retrieve testSeries report of a student",status_code=201)
async def testSeries_report(student_id:int):
    try:
        conn = Tortoise.get_connection("default")
        student_id = str(student_id)
        query = f'select us.id, us.user_id, us.class_grade_id, us.test_type, \
                   us.exam_mode, us.no_of_question, us.correct_ans, us.incorrect_ans,  \
                   us.unattmepted_ques_cnt, us.marks_gain, time_format(us.test_time,"%T") as test_time,  \
                   time_format(us.time_taken,"%T") as time_taken,  \
                   us.result_percentage, us.ans_swap_count, us.test_series_id,  \
                   ts.test_series_name, ts.series_type , \
                   date_format(us.created_at,"%d-%m-%Y") as created_at,  \
                   date_format(us.updated_at,"%d-%m-%Y") as updated_at \
                   from user_result us left join test_series ts on ts.test_series_id = us.test_series_id where us.user_id = {student_id} \
                   and us.test_series_id >0'

        summary = await conn.execute_query_dict(query)

        return JSONResponse(status_code=200,content={"success": True, "message": 'testseries report', "report": summary})
    except Exception as e:
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})



