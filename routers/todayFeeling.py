from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from db.engine import db_connection

from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from schemas.todayFeeling import todayFeeling


router = APIRouter(
    prefix='/api',
    tags=['today-feeling'],
)


@router.put("/today-feeling")
async def todayFeeling(data:todayFeeling):
    try:
        conn = Tortoise.get_connection("default")
        query = f'insert into user_analytics(user_id,user_mood_ind)values({data.user_id},{data.user_mood_ind})'
        await conn.execute_query(query)
        return JSONResponse(status_code=200,content={"massage": "inserted successfully", "success": True})
    except Exception as e:
        return JSONResponse(status_code=400,content={"message": "error","success":False})

