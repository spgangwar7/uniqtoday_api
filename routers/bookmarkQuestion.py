from http import HTTPStatus
from typing import List
import pandas as pd
import traceback
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from tortoise.models import Model
from tortoise.transactions import in_transaction
from schemas.BookmarkQuestion import BookmarkQuestion
from datetime import datetime,time


router = APIRouter(
    prefix='/api/bookmark-questions',
    tags=['Question Bookmark'],
)

@router.get('/{student_id}/{exam_id}', description='get bookmarks by student_id and exam_id')
async def question_bookmarks(student_id:int=0,exam_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query1 = f'select question_bank_name from class_exams where id={exam_id}'
        value = await conn.execute_query_dict(query1)
        questionBankDict=value[0]
        question_bank_name = questionBankDict['question_bank_name']
        query = f'select ques.question_id,ques.question,ques.template_type,ques.explanation,ques.reference_text,ques.subject_id as subt_id,ques.tags,' \
            f'ques.question_options,ques.answers,ques.passage_inst_ind,passage_inst_id,passage.passage_inst,passage.pass_inst_type,' \
            f'ques.class_id,ques.difficulty_level,ques.language_id ' \
            f'from {question_bank_name} as ques left join question_bank_passage_inst as passage on passage.id=ques.passage_inst_id ' \
            f'left join student_question_tagged as sqt on sqt.question_id=ques.question_id where student_id={student_id} group by sqt.question_id'
        result = await conn.execute_query_dict(query)
        if not result:
            return JSONResponse(status_code=400,content={"message": f"no bookmarkQuestions for user_id : {student_id} and exam_id : {exam_id}","success":False})
        return JSONResponse(status_code=200,content={'message': "Bookmarks Questions", 'response': result,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={'message': f"{e}","success":False})

@router.delete('/{student_id}/{exam_id}/{question_id}', description='delete one bookmark by student_id, exam_id and question_id', status_code=201)
async def removeBookmarks(student_id: int=0,exam_id:int=0,question_id: int=0):
    conn = Tortoise.get_connection("default")
    try:
        query = 'delete from student_question_tagged where student_id={} and exam_id={} and question_id={}'.format(
            student_id, exam_id, question_id)
        value = await conn.execute_query_dict(query)
        return JSONResponse(status_code=200,content={'message': 'question removed from bookmarks successfully', 'success': True})
    except Exception as e:
        return JSONResponse(status_code=400,content={'error': f'{e}', 'success': False})

@router.post('', description='add Question Bookmark', status_code=201)
async def addBookmarkQuestion(bk:BookmarkQuestion):
    try:
        student_id = bk.student_id
        exam_id = bk.exam_id
        question_id = bk.question_id
        subject_id = bk.subject_id
        chapter_id=bk.chapter_id
        now = datetime.now()
        conn = Tortoise.get_connection("default")
        params=(student_id, exam_id, question_id, subject_id, now)
        query = f'insert into student_question_tagged(student_id,exam_id,question_id,subject_id,chapter_id,tagged_date)values("{student_id}","{exam_id}","{question_id}","{subject_id}","{chapter_id}","{now}")'
        try:
            result = await conn.execute_query_dict(query)
        except:
            query1 = 'update table student_question_tagged set student_id={},exam_id={},question_id={},subject_id={},chapter_id{}'.format(
                student_id, exam_id, question_id, subject_id,chapter_id)
            result = await conn.execute_query_dict(query1)
        return JSONResponse(status_code=200,content={'message': 'Question bookmarked Successfully', 'success': True})
    except Exception as e:
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})
