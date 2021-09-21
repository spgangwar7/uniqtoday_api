import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
from tortoise import Tortoise
from datetime import datetime,timedelta
import json
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

@router.post("/update-leadershipBoard/{exam_id}")
async def updateLeadershipBoard(exam_id:int=0):
    try:
        start_time=datetime.now()
        conn=Tortoise.get_connection('default')
        query=f"SELECT DISTINCT(user_id),result_percentage as score FROM user_result WHERE test_type='Assessment' AND class_grade_id={exam_id} GROUP BY user_id"
        all_student_res=await conn.execute_query_dict(query)
        all_student_result=pd.DataFrame(all_student_res)
        all_student_result=all_student_result.sort_values('score',axis = 0, ascending = False)
        all_student_result['rank'] = all_student_result['score'].rank(ascending=False, method='dense')
        all_student_result['rank']=all_student_result['rank'].astype(int)
        all_student_result=all_student_result.sort_values('rank', ascending = True)
        print(all_student_result)
        res=all_student_result.to_dict('records')
        month=datetime.now().month
        year=datetime.now().year
        print(f"month: {month}, year: {year}")
        for data in res:
            query=f"INSERT INTO leadership_board(user_id,month,year,class_exam_id,marks,user_rank,created_at,updated_at)values({int(data['user_id'])},{month},{year},{exam_id},{int(data['score'])},{int(data['rank'])},NOW(),NOW())"
            #print(query)
            await conn.execute_query(query)
        print(f"execution time for this api is: {(datetime.now()-start_time)}")
        return JSONResponse(status_code=200,content={"response":f"Leadership board updated for exam_id {exam_id}","success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})

@router.get('/get-leadershipBoard/{user_id}/{exam_id}', description='Get Leadership Board by userId', status_code=201)
async def getleadership_board(user_id:int=0,exam_id:int=0):
    try:
        conn=Tortoise.get_connection('default')
        month=datetime.now().month
        year=datetime.now().year
        query=f'SELECT lb.user_id,su.user_name,lb.marks as score,lb.user_rank, su.user_profile_img FROM leadership_board lb inner join student_users as su on lb.user_id=su.id where lb.month={month} and lb.year={year} and class_exam_id={exam_id}'
        res=await conn.execute_query_dict(query)
        if len(res)==0:
            return JSONResponse(status_code=400,content={"response":f"Leadership board does not exist for this month","success":False})
        resultdf=pd.DataFrame(res)
        resultdf = resultdf.fillna("")
        res = resultdf.to_dict("records")
        last_rank=0
        if  any(resultdf.user_id==user_id):
            print("user id exists in leaderboard")
            last_rank=resultdf.iloc[-1]
            last_rank=last_rank.to_dict()
            last_rank=last_rank['user_rank']
            print(last_rank)
            user_rank_df =resultdf.loc[resultdf["user_id"]==user_id]
            user_rank_df=user_rank_df.fillna("")
            user_rank=user_rank_df.to_dict("records")
            print(user_rank)
            return JSONResponse(status_code=200, content={"response": res,"current_user":user_rank, "success": True})
        else:
            profilequery = f' SELECT id,user_name,user_profile_img FROM student_users where id={user_id}'
            profile_res = await conn.execute_query_dict(profilequery)
            profile_res=pd.DataFrame(profile_res)
            profile_res=profile_res.fillna("")
            profile_res['score']=0
            profile_res['user_rank']=last_rank
            profile_res=profile_res.to_dict("records")
            return JSONResponse(status_code=200,content={"response":res,"current_user":profile_res,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})

@router.get('/search-friend/{exam_id}/{friend_name}',description="search friend's name in leadership_board")
async def searchFriend(exam_id:int=0,friend_name:str=None):
    conn=Tortoise.get_connection('default')
    month = datetime.now().month
    year = datetime.now().year
    query = f'SELECT lb.user_id,su.first_name,su.user_name,lb.marks as score,lb.user_rank, su.user_profile_img FROM leadership_board lb inner join student_users as su on lb.user_id=su.id where lb.month={month} and lb.year={year} and lb.class_exam_id={exam_id} and su.first_name="{friend_name}"'
    res = await conn.execute_query_dict(query)
    if not res:
        resp = {
            "response": "No matching results found",
            "success": False
        }
        return JSONResponse(status_code=400, content=resp)
    resp={
        "response":res,
        "success":True
    }
    return JSONResponse(status_code=200,content=resp)
