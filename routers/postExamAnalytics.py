import json
import traceback
from http import HTTPStatus
from typing import List

import numpy as np
import pandas as pd
import redis
from fastapi import APIRouter,BackgroundTasks
from IPython.display import display, HTML
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import IntegrityError
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from datetime import datetime
from schemas.SaveResult import SaveResult
router = APIRouter(
    prefix='/api',
    tags=['Post-Exam-Analytics'],
)
@router.get("/post-exam-analytics/{user_id}/{exam_id}")
async def postExamAnalytics(user_id:int=0,exam_id:int=0):
    start_time=datetime.now()
    conn=Tortoise.get_connection('default')
    result_id=0
    r=redis.Redis()
    if r.exists(str(user_id)+"_sid"+"_result_data"):
        student_cache=json.loads(r.get(str(user_id)+"_sid"+"_result_data"))
        result_id=student_cache['result_id']
        no_of_question=student_cache["no_of_question"]
        correct_count=student_cache["correct_count"]
        correct_score=student_cache["correct_score"]
        wrong_count=student_cache["wrong_count"]
        incorrect_score=student_cache["incorrect_score"]
        total_exam_marks=student_cache["total_exam_marks"]
        total_get_marks=student_cache["total_get_marks"]
        result_time_taken=student_cache["result_time_taken"]
        result_percentage=student_cache["result_percentage"]
        not_answered=student_cache["not_answered"]
    else:
        return JSONResponse(status_code=400,content={"response":f"user {user_id} did'nt give any exam"})

    subjectquery = f'SELECT sqa.subject_id, subjects.subject_name, count(*) as total_questions,sum(attempt_status="Correct") as correct_count, sum(attempt_status="Incorrect") as incorrect_count,sum(attempt_status="Unanswered") as unanswered_count FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id  where student_result_id={result_id} group by subject_id;'
    resultbysubject = await conn.execute_query_dict(subjectquery)
    resultbysubject=pd.DataFrame(resultbysubject)
    resultbysubject=resultbysubject.fillna(0)
    resultbysubject = resultbysubject.astype({"correct_count": int, "incorrect_count": int,"unanswered_count": int,"total_questions": int})

    # Get stats of result by topic
    topicquery = f'SELECT sqa.subject_id,topic_id ,topics.topic_name,  count(*) as total_questions,sum(attempt_status="Correct") as correct_count, sum(attempt_status="Incorrect") as incorrect_count,sum(attempt_status="Unanswered") as unanswered_count FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id left join topics on sqa.topic_id =topics.id where student_result_id={result_id} group by subject_id,topic_id order by subject_id;'
    resultbytopic = await conn.execute_query_dict(topicquery)
    resultbytopic = pd.DataFrame(resultbytopic)
    resultbytopic=resultbytopic.fillna("")
    resultbytopic = resultbytopic.astype({"correct_count": int, "incorrect_count": int,"unanswered_count": int,"total_questions": int})
    resultbytopic = resultbytopic.fillna("")
    resultbytopic = resultbytopic.to_dict("records")

    # Graph of class average  and student subject wise score
    query = f'SELECT sqa.subject_id, (sum(attempt_status="Correct")/count(attempt_status)*100) as student_score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} and class_exam_id={exam_id} group by subject_id;'
    student_score = await conn.execute_query_dict(query)
    student_score = pd.DataFrame(student_score)

    classquery = f'SELECT sqa.subject_id,(sum(attempt_status="Correct")/count(attempt_status)*100) as class_score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where class_exam_id={exam_id} group by subject_id;'
    class_score = await conn.execute_query_dict(classquery)
    class_score = pd.DataFrame(class_score)

    query3 = f'SELECT subject_id,subject_name FROM exam_subjects as es inner join subjects on es.subject_id=subjects.id where class_exam_id={exam_id}'
    subjectslist = await conn.execute_query_dict(query3)
    subjectslist = pd.DataFrame(subjectslist)
    scoredf = pd.merge(subjectslist, class_score, on='subject_id', how="left")
    scoredf = pd.merge(scoredf, student_score, on='subject_id', how="left")
    scoredf = scoredf.fillna(0)

    scoredf = scoredf.astype({"class_score": float, "student_score": float})
    query1 = f"SELECT DISTINCT(user_id),(result_percentage),created_at as test_date FROM user_result WHERE class_grade_id={exam_id}  group by created_at ORDER BY result_percentage DESC "

    val1 = await conn.execute_query_dict(query1)
    df = pd.DataFrame(val1)
    df.rename(columns={"result_percentage": "score"}, inplace=True)
    filt = df['user_id'] == user_id
    v = df[filt]['score'].max()
    a = df['user_id'].unique()
    i, = np.where(a == user_id)

    class_average_query=f'SELECT avg(marks_gain) as class_average FROM user_result where class_grade_id={exam_id};'
    class_average = await conn.execute_query_dict(class_average_query)
    class_average = class_average[0].get("class_average")
    class_average=float(class_average)
    resp = {
        "no_of_question":no_of_question,
        "correct_count":correct_count,
        "correct_score":correct_score,
        "wrong_count":wrong_count,
        "incorrect_score":incorrect_score,
        "total_exam_marks":total_exam_marks,
        "total_get_marks":total_get_marks,
        "class_average":class_average,
        "result_time_taken":result_time_taken,
        "result_percentage":result_percentage,
        "not_answered":not_answered,
        "result_id": int(result_id),
        "subject_wise_result": resultbysubject.to_dict('records'),
        "topic_wise_result": resultbytopic,
        "subject_graph": scoredf.to_dict(orient='records'),
        "total_participants": int(len(a)),
        "user_rank": int(i[0] + 1),
        "success": True

    }
    print(f"execution time {datetime.now()-start_time}")
    return JSONResponse(status_code=200,content={"response":resp})

