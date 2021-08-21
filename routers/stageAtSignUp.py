from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from db.engine import db_connection

from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from schemas.stageAtSignUp import stageAtSignUp


router = APIRouter(
    prefix='/api',
    tags=['StageAtSignUp'],
)


@router.put('/stage-at-signUp',description="store user data at sign up",status_code=201)
async def stageAtSignUp(data:stageAtSignUp):
    try:
        conn=Tortoise.get_connection("default")
        query = f"update student_preferences set student_stage_at_sgnup={data.student_stage_at_sgnup} where student_id={data.student_id}"
        await conn.execute_query(query)
        return JSONResponse(status_code=200,content={"message":"student at sign up","response":"user updated successfully","success":True})
    except Exception as e:
        return JSONResponse(status_code=400,content={"reponse":"user not updated","success":False})
