import json
import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
router = APIRouter(
    prefix='/api/studentDashboard',
    tags=['Student Dashboard'],
)
"""
@router.get('/test-score/{user_id}', description='Get Test Score', status_code=201)
async def get_Test_Score(user_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select id,test_type,exam_mode,marks_gain,result_percentage from user_result where user_id={user_id} order by id desc limit 2'
        val = await conn.execute_query_dict(query)
        return JSONResponse(status_code=200,content={'message': "Result List", 'response': val,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})

@router.get('/student_proficiency/{user_id}', description='Get Subject Wise Student Proficiency', status_code=201)
async def get_student_proficiency(user_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query2 = f'SELECT grade_id FROM student_users where id={user_id} limit 1;'
        class_exam_id = await conn.execute_query_dict(query2)
        class_exam_id = [class_exam_id_dict['grade_id'] for class_exam_id_dict in class_exam_id]
        query = f'SELECT sqa.subject_id,subject_name, count(attempt_status) as total_questions,sum(attempt_status="Correct") as correct_ans,(sum(attempt_status="Correct")/count(attempt_status)*100) as score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} and class_exam_id={class_exam_id[0]} group by subject_id;'
        val = await conn.execute_query_dict(query)

        query3=f'SELECT subject_id,subject_name FROM exam_subjects as es inner join subjects on es.subject_id=subjects.id where class_exam_id={class_exam_id[0]}'
        subjectslist = await conn.execute_query_dict(query3)
        for subjectslist_dict in subjectslist:
            if not any(d['subject_id'] == subjectslist_dict['subject_id'] for d in val):
                val.append({'subject_id': subjectslist_dict['subject_id'], 'subject_name': subjectslist_dict['subject_name'], 'total_questions': 0, 'correct_ans': 0, 'score': 0})
        val=pd.DataFrame(val)
        return JSONResponse(status_code=200,content={"response":val.to_json(orient='records',date_format='iso'),"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})

@router.get('/student_class_score/{user_id}', description='Get Subject Wise Student Score- Post Exam Analytics', status_code=201)
async def get_class_score(user_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query2 = f'SELECT grade_id FROM student_users where id={user_id} limit 1;'
        class_exam_id = await conn.execute_query_dict(query2)
        class_exam_id = [class_exam_id_dict['grade_id'] for class_exam_id_dict in class_exam_id]
        query = f'SELECT sqa.subject_id,subject_name, (sum(attempt_status="Correct")/count(attempt_status)*100) as student_score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} and class_exam_id={class_exam_id[0]} group by subject_id;'
        student_score = await conn.execute_query_dict(query)
        student_score=pd.DataFrame(student_score)

        classquery = f'SELECT sqa.subject_id,(sum(attempt_status="Correct")/count(attempt_status)*100) as class_score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where class_exam_id={class_exam_id[0]} group by subject_id;'
        class_score = await conn.execute_query_dict(classquery)
        class_score=pd.DataFrame(class_score)
        scoredf=pd.merge(class_score, student_score, on='subject_id',how="left")
        scoredf=scoredf.fillna(0)
        scoredf = scoredf.astype({"class_score": float, "student_score": float})

        print(scoredf)
        return JSONResponse(status_code=200,content={"response":scoredf.to_dict(orient='records'),"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get('/marks_trend/{user_id}', description='Get Marks Trend', status_code=201)
async def get_marks_trend(user_id: int = 0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'SELECT class_grade_id FROM user_result where user_id={user_id} limit 1'
        val = await conn.execute_query_dict(query)
        class_grade_dict=val[0]
        class_grade_id=class_grade_dict["class_grade_id"]
        #print(class_grade_id)

        query2 = f'SELECT marks_gain,created_at as test_date FROM user_result where DATE(created_at) >= DATE(NOW()) - INTERVAL 28 DAY and user_id={user_id} and class_grade_id={class_grade_id};'
        result = await conn.execute_query_dict(query2)
        resultdf=pd.DataFrame(result)
        resultdf=resultdf.round(1)
        #print(resultdf)
        output=resultdf.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.max().reset_index()
        query3 = f'SELECT user_id,marks_gain,created_at as test_date FROM user_result where DATE(created_at) >= DATE(NOW()) - INTERVAL 28 DAY and class_grade_id={class_grade_id};'
        result2 = await conn.execute_query_dict(query3)
        resultdf2=pd.DataFrame(result2)
        resultdf2=resultdf2.round(2)

        output1=resultdf2.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.mean().reset_index()
        output2=resultdf2.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.max().reset_index()
        resp={
            "student-score":output.to_json(orient='records',date_format='iso'),
            "average-score":output1.to_json(orient='records',date_format='iso'),
            "max-score":output2.to_json(orient='records',date_format='iso'),
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})

"""
@router.get('/analytics/{user_id}', description='Get Test Score', status_code=200)
async def get_analytics(user_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select id,test_type,exam_mode,marks_gain,result_percentage from user_result where user_id={user_id} order by id desc limit 2'
        val = await conn.execute_query_dict(query)
        query2 = f'SELECT grade_id FROM student_users where id={user_id} limit 1;'
        class_exam_id = await conn.execute_query_dict(query2)
        class_exam_id = [class_exam_id_dict['grade_id'] for class_exam_id_dict in class_exam_id]
        print(class_exam_id)
        if not class_exam_id:
            print("User does not exists")
            return JSONResponse(status_code=400,content={'message': "User does not exists","success":False})
        if class_exam_id[0]==None:
            print("User does not have any subscription")
            return JSONResponse(status_code=400, content={'message': "User does not have any subscription", "success": False})
        query1 = f'SELECT sqa.subject_id,subject_name, count(attempt_status) as total_questions,sum(attempt_status="Correct") as correct_ans,(sum(attempt_status="Correct")/count(attempt_status)*100) as score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} and class_exam_id={class_exam_id[0]} group by subject_id;'
        subject_proficiency = await conn.execute_query_dict(query1)
        subject_proficiency=pd.DataFrame(subject_proficiency)
        #print(subject_proficiency)
        if not subject_proficiency.empty:
            subject_proficiency['score'] = subject_proficiency['score'].astype(float)
            subject_proficiency['correct_ans'] = subject_proficiency['correct_ans'].astype(float)

            subject_proficiency = subject_proficiency.to_dict(orient="records")
        else:
            subject_proficiency=[]
        query3 = f'SELECT subject_id,subject_name FROM exam_subjects as es inner join subjects on es.subject_id=subjects.id where class_exam_id={class_exam_id[0]}'
        subjectslist = await conn.execute_query_dict(query3)
        for subjectslist_dict in subjectslist:
            if not any(d['subject_id'] == subjectslist_dict['subject_id'] for d in subject_proficiency):
                subject_proficiency.append(
                    {'subject_id': subjectslist_dict['subject_id'], 'subject_name': subjectslist_dict['subject_name'],
                     'total_questions': 0, 'correct_ans': 0, 'score': 0})

        query_t = f'SELECT class_grade_id FROM user_result where user_id={user_id} limit 1'
        val_t = await conn.execute_query_dict(query_t)
        print(val_t)
        if val_t:
            class_grade_dict = val_t[0]
            class_grade_id = class_grade_dict["class_grade_id"]
        # print(class_grade_id)
        # finding marks trend
        query2 = f'SELECT marks_gain,created_at as test_date FROM user_result where DATE(created_at) >= DATE(NOW()) - INTERVAL 28 DAY and user_id={user_id} and class_grade_id={class_exam_id[0]};'
        result = await conn.execute_query_dict(query2)
        resultdf = pd.DataFrame(result)
        if not resultdf.empty:
            output = resultdf.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.max().to_dict()
        else:
            output={}
        resultdf = resultdf.round(1)
        query3 = f'SELECT user_id,marks_gain,created_at as test_date FROM user_result where DATE(created_at) >= DATE(NOW()) - INTERVAL 28 DAY and class_grade_id={class_exam_id[0]};'
        result2 = await conn.execute_query_dict(query3)
        resultdf2 = pd.DataFrame(result2)
        resultdf2 = resultdf2.round(2)
        output1 = resultdf2.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.mean().to_dict()
        output1 = {k: round(v, 2) for k, v in output1.items()}
        output2 = resultdf2.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.max().to_dict()
        finaldict = []
        for key, value in output1.items():
            # print("key"+str(key))
            student_score = output.get(key, 0)
            average_score = output1.get(key, 0)
            max_score = output2.get(key, 0)

            if pd.isna(student_score) :
                student_score=0
            if pd.isna(average_score) :
                average_score=0
            if pd.isna(max_score) :
                max_score=0
            finaldict.append({"date":str(key),"student_score": student_score, "average_score": average_score, "max_score": max_score})
            #finaldict[key] = {"student_score": student_score, "average_score": average_score, "max_score": max_score}
        # print(finaldict)
        # print(output2)
        resp={'test_score': val,'subject_proficiency': subject_proficiency,'marks_trend':finaldict,"success":True}
        #print(resp)
        return json.dumps(resp)

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})