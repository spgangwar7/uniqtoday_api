import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi.responses import FileResponse
from tortoise import Tortoise
from tortoise.queryset import QuerySet
from fastapi import APIRouter,File, UploadFile
from fastapi.encoders import jsonable_encoder
from db.engine import db_connection
from models.UserModel import User_Pydantic, UserIn_Pydantic, User
from tortoise.exceptions import  *
from tortoise.query_utils import Q
from fastapi.responses import JSONResponse
from schemas.Users import UpdateUsers
router = APIRouter(
    prefix='/api',
    tags=['Users'],
)

@router.post('/users', description='create user', status_code=201)
async def create_user(userp: UserIn_Pydantic):
    try:
        if await User.filter(Q(email__contains=userp.email)|Q(mobile__contains=userp.mobile)).exists():
            print("User already exists")
            return JSONResponse(status_code=400,content={'message': "User already exists","success":False})
        else:
            user_obj = await User.create(**userp.dict(exclude_unset=True))
            return JSONResponse(status_code=200,content={"message": "User saved successfully", "response": user_obj,"success":True})
        #return await User_Pydantic.from_tortoise_orm(user_obj)

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={'error': f"{e}","success":False})


@router.put('/users', description='create user', status_code=201)
async def update_user(userp: UpdateUsers):
    try:
        conn=Tortoise.get_connection("default")
        query = f"update student_users set first_name='{userp.first_name}',user_name='{userp.user_name}',last_name='{userp.last_name}',email='{userp.email}' ,mobile='{userp.mobile}' where id={userp.id}"
        print(query)
        result=await conn.execute_query(query)
        return JSONResponse(status_code=200,content={"response":"user updated successfully","success":True})

        #return await User_Pydantic.from_tortoise_orm(user_obj)

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})

@router.get('/user-subscription/{student_id}',description="get all the subscriptions of a user")
async def users_subscription(student_id:int=0):
    conn=Tortoise.get_connection('default')
    #query = f'SELECT subscription_name, a.purchase_date,a.subscription_start_date, a.subscription_end_date,a.exam_year from users_purchase a, subscriptions_for_sale b  where user_id = {student_id}'

    query=f"select sf.subscription_name,up.purchase_date,up.subscription_start_date,up.subscription_end_date from users_purchase up join subscriptions_for_sale sf on up.subscription_id=sf.id where up.user_id={student_id}"

    res = await conn.execute_query_dict(query)
    res=pd.DataFrame(res)
    #res['name']=res['user_name']
    return JSONResponse(status_code=200,content={"response":res.to_json(orient="records", date_format='iso'),"success":True})

"""
@router.post("/update-profile-picture/")
async def update_profile_picture(student_id:int=0,file: UploadFile = File(...)):
    import os.path
    extension = os.path.splitext(file.filename)[1]
    print(extension)
    if extension!= ".jpg" or ".jpeg" or ".png" or ".tif" or ".tiff":
        return {"message":"Please upload a valid image file","success":False}
    file_name = f'images/profile/' + str(student_id)+extension
    with open(file_name, 'wb+') as f:
        f.write(file.file.read())
        f.close()
    return {"message":"File uploaded successfully","filename": file_name,"success":True}

@router.get("/profile-picture/{student_id}")
async def get_profile_picture(student_id:int=0):
    import os.path
    file_name = f'images/profile/' + str(student_id)+".png"
    if os.path.exists(file_name):
        print ("File exists")
    else :
        print ("File does not exists")
        return {"message":"File does not exists"}
    return FileResponse(file_name)
    #return {"message":"File uploaded successfully","filename": file_name,"success":True}
"""