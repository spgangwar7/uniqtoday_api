import traceback
import pandas as pd
from fastapi import APIRouter
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse


router = APIRouter(
    prefix='/api/previous-year-question-paper',
    tags=['previousYearQuestionPapers'],
)
@router.get('/download/{exam_year}/{exam_id}/{subject_id}',description='download previous year question papers',status_code=201)
async def PrevYearQuestPaper(exam_year:int=0,exam_id:int=0,subject_id:int=0):
    try:
        conn = Tortoise.get_connection('default')
        query = f'select paper_file_name from exam_rev_year_ques_papers where exam_year={exam_year} and exam_id={exam_id} and subject_id={subject_id}'
        val = await conn.execute_query_dict(query)
        val = pd.DataFrame(val)
        print(val)
        if val.empty:
            return JSONResponse(status_code=400,content={"response": "No Data Available","success":False})
        return JSONResponse(status_code=200,content={"response": val.to_json(orient='records'),"success":True})
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})





