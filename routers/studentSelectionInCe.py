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
from schemas.StudentSelectionInCe import StudentSelectionInCe
import pickle
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
import time

import json
router = APIRouter(
    prefix='/api',
    tags=['Student-Selection-In-Competative-Exam'],
)
path_pickle='pickle/'

@router.post('/Student-Selection-In-Competative-Exam',description='this API predicts the probability of selection of individual student based on past performances into three brackets(low,medium,high)')
def StudentSelectionInCE(data:StudentSelectionInCe):
    try:
        start_time = time.time()
        getJson = jsonable_encoder(data)
        df = pd.DataFrame([getJson])
        # get all the input parameters from Post request
        df = df[['age', 'gender', 'school', 'study_time', 'no_of_attempts',
                 'total_practice_tests_taken',
                 'extra_school_classes', 'extra_curriculars', 'time_sm',
                 'free_time', 'go_out', 'avg_perf_practice_tests', 'travel_time',
                 'city_name', 'state_name']]
        yes_no = {"Yes": 1, "No": 0}
        gender = {"F": 1, "M": 0}
        school = {"Cambridge": 0, "Carmel Convent School": 1, "Delhi Public School": 2, "Kendriya Vidyalaya": 3,
                  "Podar": 4, "St Johns": 5}
        state_name = {"Delhi": 0, "Karnataka": 1, "Maharashtra": 2, "Uttar Pradesh": 3}
        city_name = {"Bangalore": 0, "Delhi": 1, "Lucknow": 2, "Mumbai": 3, "Pune": 4}
        df["extra_school_classes"] = df["extra_school_classes"].apply(lambda x: yes_no.get(x, 0))
        df["extra_curriculars"] = df["extra_curriculars"].apply(lambda x: yes_no.get(x, 0))
        df["gender"] = df["gender"].apply(lambda x: gender.get(x, 0))
        df["school"] = df["school"].apply(lambda x: school.get(x, 0))
        df["city_name"] = df["city_name"].apply(lambda x: city_name.get(x, 0))
        df["state_name"] = df["state_name"].apply(lambda x: state_name.get(x, 0))
        scaler = MinMaxScaler()
        scaled_df = scaler.fit_transform(df)
        scaled_df = pd.DataFrame(scaled_df, columns=df.columns)
        valuesToPredict = np.asarray(scaled_df)

        # Load from file
        pkl_filename = path_pickle + "Students_Selection_CE.pkl"
        with open(pkl_filename, 'rb') as file:
            pickle_model = pickle.load(file)
        probability_percentage = pickle_model.predict_proba(scaled_df)
        probability_df = pd.DataFrame(data=probability_percentage, columns=['Low', 'Medium', 'High'])
        print(probability_df)
        # Based on amount of request get all the probabilites as per model
        d = {}
        finalDict = {}
        for index, row in probability_df.iterrows():
            for i in ['Low', 'Medium', 'High']:
                val = row[i]
                val = format(val, '.2f')
                if (eval(val) > 0.00):
                    d.update({i: val})
            finalDict.update({"Selection_Chance" + "_student" + str(index): d})
            d = {}
        print("Time took for execution for this API: %s seconds " % (time.time() - start_time))
        return JSONResponse(status_code=200, content={"response": json.dumps(finalDict), "success": True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}', "success": False})