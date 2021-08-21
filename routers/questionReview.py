import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
import numpy as np
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import IntegrityError
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from tortoise.models import Model
from tortoise.transactions import in_transaction

router = APIRouter(
    prefix='/api',
    tags=['Question Reviews'],
)


@router.get('/question-reviews/{result_id}', description='get question reviews')
async def questionReviews(result_id: int = 0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select user_id,class_grade_id as class_id from user_result where id ={result_id}'
        result = await conn.execute_query_dict(query)
        if result:

            #print(result)
            user_id = result[0]['user_id']
            class_id = result[0]['class_id']
            #print(class_id)
            query1 = f'select question_bank_name from class_exams where id = {class_id}'
            qbanktable = await conn.execute_query_dict(query1)
            qbanktable=qbanktable[0]['question_bank_name']
            query3 = f'select distinct question_id from student_questions_attempted where student_result_id = {result_id}'
            question_idlist = await conn.execute_query_dict(query3)
            if question_idlist:
                question_idframe = pd.DataFrame(question_idlist)
                questionids_string1=question_idframe['question_id'].values.tolist()
                first = question_idframe['question_id'].iloc[0]
                last = question_idframe['question_id'].iloc[-1]
                question_idframe = question_idframe.loc[:,'question_id']
                questionids=question_idframe.values
                questionids_string = ", ".join(map(str, questionids))
                #print(questionids)
                query4 = f'select qtable.question_id,qtable.subject_id,question,tags,difficulty_level,template_type,skill_id, \
                         atm.attempt_status,atm.option_id,language_id,explanation, \
                         question_options,answers,reference_text from {qbanktable} as qtable left join  student_questions_attempted \
                         as atm on atm.question_id = qtable.question_id where qtable.question_id IN ({questionids_string}) and student_result_id = {result_id}'
                final_result = await conn.execute_query_dict(query4)
                resp={"question_ids": questionids_string1,"all_question":final_result,"first":str(first),"last":str(last),"success":True}
            else:
                resp={"message": "Data does not exist for this result id","sucess":False}
                return JSONResponse(status_code=400, content=resp)

        else:
            resp = {"message": "Result id does not exist, please try again with different result id","success":False}
            return JSONResponse(status_code=400, content=resp)
        return JSONResponse(status_code=200,content=resp)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error": f"{e}","success":False})
