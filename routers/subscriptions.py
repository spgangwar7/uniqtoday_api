import traceback
from builtins import print
from datetime import datetime, time, date
import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import IntegrityError
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from schemas.Subscriptions import SavePurchaseDetails,SaveTrialSubscription
import schedule

router = APIRouter(
    prefix='/api',
    tags=['Subscriptions'],
)

@router.get('/subscriptions/{user_id}', description='get user purchase history')
async def mySubscriptions(user_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select purchase.*,sale.subscription_name from users_purchase as purchase ' \
            f'left join subscriptions_for_sale as sale on sale.id=purchase.subscription_id where purchase.user_id={user_id} order by purchase.user_pur_seq'
        val = await conn.execute_query_dict(query)
        val=pd.DataFrame(val)
        return JSONResponse(status_code=200,content={'order_details': val.to_json(orient='records',date_format="iso"),"success":True})
    except Exception as e:
        return JSONResponse(status_code=400,content={"error": f"{e}","success":False})

@router.get("/subscription-packages/{student_id}", description="Get Subscription Packages List, Student ID is optional")
async def subscriptionPackages(student_id:int=0):
    try:
        conn=Tortoise.get_connection("default")
        today_date =datetime.today().strftime('%Y-%m-%d')
        t_date=datetime.today()
        exam_year = t_date.year
        query = f'select sbsc.exam_year as exam_year,sbsc.id as subscript_id,sbsc.subscription_name,sbsc.subscription_details,sbsc.subs_price as subs_price, sbsc.subs_dis_price as subs_dis_price,subs_valid_upto,sbsc.subs_status,sbsc.class_exam_id,sbsc.subs_type from subscriptions_for_sale as sbsc where sbsc.exam_year in {(exam_year,exam_year+1)} order by sbsc.exam_year asc '
        all_sub = await conn.execute_query_dict(query)
        all_sub_df=pd.DataFrame(all_sub)
        print(all_sub_df)
        all_sub=all_sub_df.to_dict('records')
        if student_id!=0:
            query1 = f'select user_pur_seq,user_id,subscription_id,subscription_start_date,subscription_end_date,subscription_type from users_purchase as purch where purch.user_id={student_id} and purch.subscription_end_date >= NOW() order by purch.user_pur_seq desc'
            pur_sub =await conn.execute_query_dict(query1)
            #print(query1)
            pur_sub_df=pd.DataFrame(pur_sub)
            return {'message':'Subscription Packages','Exam Year':exam_year,'all_packages':all_sub_df.to_json(orient='records',date_format="iso"),'purchased_packages':pur_sub_df.to_json(orient='records',date_format="iso"),'success':True}

        #return {'message': 'Subscription Packages', 'Exam Year': exam_year, 'all_packages': all_sub, 'success': True}
        return JSONResponse(status_code=200,content={'message': 'Subscription Packages', 'Exam Year': exam_year, 'all_packages': all_sub_df.to_json(orient='records',date_format="iso"), 'success': True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})

@router.get("/subscription-package-detail/{package_id}", description="Get Subscription Package Details")
async def subscriptionPackageDetail(package_id:int=0):
    try:
        conn=Tortoise.get_connection("default")
        today_date =datetime.today().strftime('%Y-%m-%d')
        query = f'select * from subscriptions_for_sale where id="{package_id}"'
        package_detail = await conn.execute_query_dict(query)
        package_detail_df=pd.DataFrame(package_detail)
        print(package_detail_df['subs_type'].iloc[0])
        if package_detail_df['subs_type'].iloc[0]=='O':
            expirydate=package_detail_df['subs_valid_upto'].iloc[0]
            todaysdate=datetime.today()
            num_months = (expirydate.year - todaysdate.year) * 12 + (expirydate.month - todaysdate.month)
            package_detail_df['months']=num_months
            return {'message': 'Subscription Package Details','package_details': package_detail_df.to_dict('records'),'success':True}
        else:
            return {'message': 'Subscription Package Details','package_details': package_detail_df.to_dict('records'),'success':True}

    except Exception as e:
        print(e)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})

@router.post("/save-trial-subscription", description="Save Trial Subscription")
async def saveTrialSubscription(save_pd:SaveTrialSubscription):
    try:
        conn=Tortoise.get_connection("default")
        student_id =save_pd.student_id
        amount =0
        subscription_start_date =date.today()
        subscription_end_date=date.today() + relativedelta(days=+int(14))
        payment_date =date.today()
        subscription_id =save_pd.subscription_id
        exam_year =save_pd.exam_year
        query = f'select class_exam_id from subscriptions_for_sale where id="{subscription_id}"'
        class_exam_id=await conn.execute_query_dict(query)
        exam_id = class_exam_id[0]['class_exam_id']

        query=f'insert into users_purchase(user_id,amount,purchase_date,subscription_type,created_on,subscription_end_date,subscription_start_date,subscription_id,exam_year)' \
              f'values({student_id},{amount},"{payment_date}","T","{payment_date}","{subscription_end_date}","{subscription_start_date}",{subscription_id},{exam_year})'
        result=await conn.execute_query(query)
        if result:
            try:
                query1 = f'update student_preferences set subscription_yn={1},trial_expired_yn="Y",subscription_expiry_date="{subscription_end_date}" where student_id={student_id}'
                await conn.execute_query_dict(query1)
                query2 = f'update student_users set grade_id="{exam_id}" where id="{student_id}"'
                result1 = await conn.execute_query_dict(query2)
                return JSONResponse(status_code=200,content={"message": "Subscription Packages details stored", 'success': True})
            except Exception as e:
                return JSONResponse(status_code=400,content={"error":f"{e}","success":False})
        else:
            return JSONResponse(status_code=400,content={'message': 'Data not stored', 'success': False})

    except Exception as e:
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})


@router.post("/check-trial-subscription", description="Check if student has used trial package before")
async def checkTrialSubscription(student_id:int=0):
    try:
        conn=Tortoise.get_connection("default")
        s_id=student_id
        query=f'SELECT trial_expired_yn,subscription_expiry_date FROM learntoday_uat.student_preferences where student_id={s_id};'
        result=await conn.execute_query_dict(query)
        result=pd.DataFrame(result)
        #print(result)
        result['subscription_expiry_date']=result['subscription_expiry_date'].astype(str)
        resp={
            'message': "Trial subscription details",
            'response': result.to_dict(orient='records'),
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)
    except Exception as e:
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"error": f"{e}", "success": False})