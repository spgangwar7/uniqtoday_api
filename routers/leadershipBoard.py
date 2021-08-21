import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
from tortoise import Tortoise
from tortoise.queryset import QuerySet
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from models.LeadershipBoard import LeadershipBoardIn_Pydantic,LeadershipBoard_Pydantic,LeadershipBoard
from tortoise.exceptions import  *
from tortoise.query_utils import Q
from fastapi.responses import JSONResponse
router = APIRouter(
    prefix='/api',
    tags=['Leadership Board'],
)

@router.get('/leadershipBoard/{user_id}', description='Get Leadership Board by userId', status_code=201,response_model=List[LeadershipBoard_Pydantic])
async def getleadership_board(user_id:int):
    try:
        leadershipObj = LeadershipBoard.filter(user_id=user_id)
        res=await leadershipObj
        resp={
            "response":jsonable_encoder(res),
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})