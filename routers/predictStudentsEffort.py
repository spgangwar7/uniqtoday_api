import traceback
from http import HTTPStatus
import numpy as np
import pandas as pd
from tortoise import Tortoise
from tortoise.queryset import QuerySet
from fastapi import APIRouter,HTTPException
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from tortoise.query_utils import Q
from fastapi.responses import JSONResponse
from schemas.PredictStudentEffort import PredictStudentEfforts
import pickle
from sklearn.preprocessing import LabelEncoder
import time

import json
router = APIRouter(
    prefix='/api',
    tags=['Predict Student Efforts'],
)

path_pickle='pickle/'
@router.post('/predict-students-efforts',description="it predicts no of hours per week to study")
def PredictStudentsEfforts(data:PredictStudentEfforts):
    start_time = time.time()
    getJson=jsonable_encoder(data)
    #get all the input parameters from Post request
    df_j=pd.DataFrame([getJson])
    #handle nulll values below
    df_j['age'] = df_j['age'].apply(lambda x: "21" if (x == "null" or x == np.nan or x == "") else x)
    df_j['no_of_attempts'] = df_j['no_of_attempts'].apply(
        lambda x: "2" if (x == "null" or x == np.nan or x == "") else x)
    df_j['board_exam_marks_avg'] = df_j['board_exam_marks_avg'].apply(
        lambda x: "75" if (x == "null" or x == np.nan or x == "") else x)
    df_j['previous_test_scores'] = df_j['previous_test_scores'].apply(
        lambda x: "99" if (x == "null" or x == np.nan or x == "") else x)
    df_j['gender'] = df_j['gender'].apply(lambda x: "M" if (x == "null" or x == np.nan or x == "") else x)
    df_j['internet_accessibility'] = df_j['internet_accessibility'].apply(
        lambda x: "Y" if (x == "null" or x == np.nan or x == "") else x)
    df_j['private_tuition'] = df_j['private_tuition'].apply(
        lambda x: "Y" if (x == "null" or x == np.nan or x == "") else x)

    df_j = df_j.astype({"age": int, "no_of_attempts": int, "board_exam_marks_avg": int, 'previous_test_scores': int})

    # load the model from disk
    print(df_j)
    pkl_filename=path_pickle+'Predict_Student_Efforts_MODEL.pkl'
    with open(pkl_filename, 'rb') as file:
        model = pickle.load(file)

    y_pred = model.predict(df_j)
    df_predicted = pd.DataFrame((np.exp(y_pred.values)), columns=['predicted_no_of_hours'])
    df_predicted["predicted_no_of_hours"] = df_predicted['predicted_no_of_hours'].astype(int)
    return df_predicted.to_json(orient="records")