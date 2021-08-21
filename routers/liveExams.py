import traceback
from http import HTTPStatus
import pandas as pd
from fastapi import APIRouter
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
router = APIRouter(
    prefix='/api/live-exam',
    tags=['Live Exams'],
)
def td_to_str(td):
    """
    convert a timedelta object td to a string in HH:MM:SS format.
    """
    hours, remainder = divmod(td.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{int(hours):02}:{int(minutes):02}:{int(seconds):02}'

@router.get('/get-all', description='Get All Live Exams', status_code=201)
async def getAllLiveExams():
    try:
        conn = Tortoise.get_connection("default")
        query = 'select * from ct_exams_list where grade_id = 1 and exam_type="Live" and exam_date >= curdate()'
        val = await conn.execute_query_dict(query)
        val=pd.DataFrame(val)
        val['start_time'] = val['start_time'].apply(td_to_str)
        val['end_time'] = val['end_time'].fillna(pd.Timedelta(0))
        val['end_time'] = val['end_time'].apply(td_to_str)
        val['exam_date'] = val['exam_date'].astype(str)
        val['result_date'] = val['result_date'].astype(str)
        val['createdAt'] = val['createdAt'].dt.strftime('%Y-%m-%d')
        val['updatedAt'] = val['updatedAt'].dt.strftime('%Y-%m-%d')
        val=val.fillna(0)
        resp={
            'message': "Live Exam List",
            'response': val.to_dict(orient='records'),
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})