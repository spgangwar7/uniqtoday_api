from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from db.engine import db_connection
from models.UserModel import User_Pydantic, UserIn_Pydantic, User
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
router = APIRouter(
    prefix='/api',
    tags=['Resource'],
)


@router.get("/notes-list/{exam_id}/{subject_id}",description="get notes from resources",status_code=201)
async def noteslist(exam_id:int=0,subject_id:int=0):
    try:
        conn=Tortoise.get_connection("default")
        query = f'select resource.id,resource.class_id,resource.subject_id,resource.chapter_id,resource.topic_id,resource.resource_name,resource.resource_desc,resource.resource_type,resource.resource_file,resource.resource_link,resource.uploaded_by,resource.created_at,resource.status,resource.created_by_user,' \
                f'sub.subject_name,tpc.topic_name,ce.class_exam_cd from subject_resources as resource left join subjects as sub on sub.id=resource.subject_id ' \
                f'left join topics as tpc on tpc.id=resource.topic_id left join class_exams as ce on ce.id=resource.class_id where resource.resource_type={2} ' \
                f'and resource.class_id={exam_id} and resource.subject_id={subject_id} order by created_at desc'
        val = await conn.execute_query_dict(query)
        if len(val)==0:
            return JSONResponse(status_code=400,content={"response":"No Notes found","success":False})
        val=pd.DataFrame(val)
        val['created_at'] = pd.to_datetime(val['created_at']).dt.strftime("%d-%m-%y")
        return JSONResponse(status_code=200,content={'message':'notes list','list':val.to_dict('records'),'success':True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/presentation-list/{exam_id}/{subject_id}",description="get presentation from resource",status_code=201)
async def presentationlist(exam_id:int=0,subject_id:int=0):
    try:
        conn=Tortoise.get_connection("default")
        query = f'select resource.id,resource.class_id,resource.subject_id,resource.chapter_id,resource.topic_id,resource.resource_name,resource.resource_desc,resource.resource_type,resource.resource_file,resource.resource_link,resource.uploaded_by,resource.created_at,resource.status,resource.created_by_user,' \
                f'sub.subject_name,tpc.topic_name,ce.class_exam_cd from subject_resources as resource left join subjects as sub on sub.id=resource.subject_id ' \
                f'left join topics as tpc on tpc.id=resource.topic_id left join class_exams as ce on ce.id=resource.class_id where resource.resource_type={3} ' \
                f'and resource.class_id={exam_id} and resource.subject_id={subject_id} order by created_at desc'
        val = await conn.execute_query_dict(query)
        if len(val)==0:
            return JSONResponse(status_code=400,content={"response":"No presentation found","success":False})
        val = pd.DataFrame(val)
        val['created_at']=pd.to_datetime(val['created_at']).dt.strftime("%d-%m-%y")
        return JSONResponse(status_code=200,content={'message':'presetation list','list':val.to_dict('records'),'success':True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})

@router.get("/video-list/{exam_id}/{subject_id}",description="get video from resource",status_code=201)
async def videolist(exam_id:int=0,subject_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select resource.id,resource.class_id,resource.subject_id,resource.chapter_id,resource.topic_id,resource.resource_name,resource.resource_desc,resource.resource_type,resource.resource_file,resource.resource_link,resource.uploaded_by,resource.created_at,resource.status,resource.created_by_user,' \
                f'sub.subject_name,tpc.topic_name,ce.class_exam_cd from subject_resources as resource left join subjects as sub on sub.id=resource.subject_id ' \
                f'left join topics as tpc on tpc.id=resource.topic_id left join class_exams as ce on ce.id=resource.class_id where resource.resource_type={1} ' \
                f'and resource.class_id={exam_id} and resource.subject_id={subject_id} order by created_at desc'
        val = await conn.execute_query_dict(query)
        if len(val)==0:
            return JSONResponse(status_code=400,content={"response":"No videos found","success":False})
        val = pd.DataFrame(val)
        val['created_at'] = pd.to_datetime(val['created_at']).dt.strftime("%d-%m-%y")
        return JSONResponse(status_code=200,content={'message':'video list','list':val.to_dict('records'),'success':True})
    except IntegrityError as e:
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})
