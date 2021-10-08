import logging
import os
import traceback
from http import HTTPStatus
from typing import List

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from fastapi.responses import FileResponse
from tortoise import Tortoise
from tortoise.queryset import QuerySet
from fastapi import APIRouter, File, UploadFile, Form
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
        resp={
           "user_info":{
               "first_name": userp.first_name,
               "user_name": userp.user_name,
               "last_name": userp.last_name,
               "email": userp.email,
               "mobile": userp.mobile
           },
            "response":"user updated successfully",
            "success":True

        }
        return JSONResponse(status_code=200,content=resp)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})

@router.get('/user-subscription/{student_id}',description="get all the subscriptions of a user")
async def users_subscription(student_id:int=0):
    conn=Tortoise.get_connection('default')
    query=f"select sf.subscription_name,DATE_FORMAT(up.purchase_date,'%d-%m-%Y') as purchase_date,DATE_FORMAT(up.subscription_start_date,'%d-%m-%Y') as subscription_start_date,DATE_FORMAT(up.subscription_end_date,'%d-%m-%Y') as subscription_end_date from users_purchase up join subscriptions_for_sale sf on up.subscription_id=sf.id where up.user_id={student_id}"
    res = await conn.execute_query_dict(query)
    if not res:
        return JSONResponse(status_code=400,content={'response':f"User don't have any subscription","success":False})

    return JSONResponse(status_code=200,content={"response":res,"success":True})


from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.
FILESYSTEM_DRIVER = os.environ.get("FILESYSTEM_DRIVER")
AWS_BUCKET= os.environ.get("AWS_BUCKET")
AWS_ACCESS_KEY_ID= os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY= os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION= os.environ.get("AWS_DEFAULT_REGION")

@router.post("/update-profile-picture")
async def update_profile_picture(student_id:str= Form(...),file: UploadFile = File(...),file_extension:str=Form(...)):
    try:
        conn=Tortoise.get_connection('default')
        import os.path
        import time
        extension = os.path.splitext(file_extension)[1]
        print(student_id)
        print(file)
        print(extension)
        time_str=int(time.time())
        extension_list=[".jpg" , ".jpeg" , ".png" , ".tif" , ".tiff"]
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        if extension not in extension_list:
            return {"message":"Please upload a valid image file","success":False}
        folder_name='./images/profile'
        file_name =  str(time_str)+"_"+str(student_id)+extension
        url = f"https://{AWS_BUCKET}.s3.{AWS_DEFAULT_REGION}.amazonaws.com/{file_name}"
        try:
            file_name_upload = f'{folder_name}/{file_name}'
            with open(file_name_upload, 'wb+') as f:
                f.write(file.file.read())
                f.close()
            response = s3_client.upload_file(file_name_upload, AWS_BUCKET, file_name,ExtraArgs={'ACL':'public-read'})
            query = f"update student_users set user_profile_img='{url}' where id={student_id}"
            await conn.execute_query(query)
            os.remove(file_name_upload)
        except ClientError as e:
            logging.error(e)
            return False

        return JSONResponse(status_code=200,content={"message":"File uploaded successfully","filename": url,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f"{e}", "success": False})
"""
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