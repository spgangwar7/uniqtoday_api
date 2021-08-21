from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder

from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse


router = APIRouter(
    prefix='/api',
    tags=['preference'],
)

@router.get("/preference/{student_id}",description="get student preferences",status_code=201)
async def preferences(student_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select student_id, student_stage_at_sgnup, prof_asst_test,' \
                f'prof_test_date,prof_test_marks,share_progress_rpt_yn,' \
                f'email_id_share_rpt,rpt_share_freq,parent_module_access_yn,parent_protal_frreq,parent_portal_access_fee,' \
                f'parent_last_login_date, daily_study_hours,student_dob,' \
                f'no_of_attempts_taken,student_time_per_ques,' \
                f'question_bank_exhausted_flag,ques_exhausted_date,scholar_test_date,scholar_test_status,scholarship_test_marks,' \
                f'subscription_yn,subscription_expiry_date,previous_attempts_cnt,self_rating,planned_hours,spent_hours,total_questions,' \
                f'attempted_ques_count,correct_ans_ques_count,subject_marks_analytics,subject_ques_time_analytics,' \
                f'create_by_user_id, created_on,subject_info, subjects_rating,language_id, assessment_taken_cnt,trial_expired_yn from student_preferences where student_id={student_id}'
        val = await conn.execute_query_dict(query)
        val = pd.DataFrame(val)
        return JSONResponse(status_code=200, content={'message': 'User Preference Data',
                                                      'response': val.to_json(orient='records', date_format='iso'),
                                                      "success": True})
    except Exception as e:
        return JSONResponse(status_code=400, content={"error":f"{e}","success":False})
