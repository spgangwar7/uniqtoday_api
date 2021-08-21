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
    tags=['Goal Setting'],
)
def pct_goal_rounder(number):
    modified_number = []
    if number <= 70:
        modified_number = 70
    elif (number > 70 ) & (number <= 80):
        modified_number = 80
    elif (number > 80 ) & (number <= 100):
        modified_number = 90
    return modified_number

path_pickle='pickle/goal_setting/'
@router.get('/goal-setting',description='this API predicts whither goal set by individual students status as per past performances')
async def GoalSetting(student_id:int=0):
    try:
        start_time = time.time()
        conn = Tortoise.get_connection('default')
        query = f'select student_id,subject_id,marks_pct_goal,planned_achieve_date,date_ason from student_goals where student_id\
            ={student_id}'
        subjectwise_goal = await conn.execute_query_dict(query)
        subjectwise_goal = pd.DataFrame(subjectwise_goal)
        print(f"subjectwise_goal: {subjectwise_goal}")
        if subjectwise_goal.empty:
            return JSONResponse(status_code=400,content={"response":f"no goals exist for student_id {student_id}","success":False})
        difficuly_weights = {1: 1, 2: 1.25, 3: 1.75}
        predictions = subjects = []
        # subjectwise goals are matched with model predictions on past performances
        if subjectwise_goal.empty != True:
            for subject in subjectwise_goal['subject_id'].value_counts().index:
                try:
                    subjectwise_goal['marks_pct_goal'] = subjectwise_goal['marks_pct_goal'].astype(float)
                    subjectwise_goal['marks_pct_goal'] = subjectwise_goal['marks_pct_goal'].apply(pct_goal_rounder)
                    single_subject = subjectwise_goal[subjectwise_goal['subject_id'] == subject].reset_index(drop=True)
                    single_subject_plan_achive_date = single_subject['planned_achieve_date'].iloc[0]
                    single_subject_marks_pct_goal = single_subject['marks_pct_goal'].iloc[0]
                    query = f'Select count(distinct(created_on)) as past_attempts from student_questions_attempted where student_id\
                        ={student_id}'
                    counts = await conn.execute_query_dict(query)
                    counts = pd.DataFrame(counts)
                    print(counts)
                    if (counts['past_attempts'].iloc[0] == 0):
                        y_pred = "null"
                    elif (counts['past_attempts'].iloc[0] <= 2):
                        y_pred = "null"
                    elif (counts['past_attempts'].iloc[0] <= 10) & (counts['past_attempts'].iloc[0] > 2):
                        query = f'SELECT created_on AS test_date,difficulty_level,' \
                                f'SUM(attempt_status="Correct" OR attempt_status="Incorrect") AS attempt_cnt,' \
                                f'SUM(attempt_status="Incorrect") AS attempt_incorrect_cnt,' \
                                f'SUM(attempt_status="Correct") AS attempt_correct,' \
                                f'SUM(gain_marks) AS marksgained,' \
                                f'SUM(question_marks) AS testmarks ' \
                                f'FROM student_questions_attempted a,question_bank_master b ' \
                                f'WHERE a.question_id = b.question_id AND a.student_id = {student_id} AND a.subject_id = {subject} GROUP BY test_date, difficulty_level'
                        df_int = await conn.execute_query_dict(query)
                        df_int = pd.DataFrame(df_int)
                        df_int['difficulty_weights'] = df_int['difficulty_level'].map(difficuly_weights)
                        df_int['weight_marks'] = df_int['marksgained'] * df_int['difficulty_weights']
                        df = df_int.groupby(['test_date']).agg(
                            {'weight_marks': np.sum, 'testmarks': np.sum}).reset_index()
                        df['marks_percentage'] = (df['weight_marks'] / df['testmarks']) * 100
                        df['shifted_marks_percentage'] = df['marks_percentage'].shift(1, axis=0)
                        df['difference'] = df['marks_percentage'] - df['shifted_marks_percentage']
                        if df.shape[0] == 10:
                            score = df['marks_percentage'].iloc[-1] * df['difference'].iloc[-1]
                        else:
                            score = df['marks_percentage'].iloc[-1] * df['difference'].iloc[-1] * (10 - df.shape[0])
                        if (single_subject_marks_pct_goal == 70) & (score > 65):
                            y_pred = 1
                        elif (single_subject_marks_pct_goal == 80) & (score > 75):
                            y_pred = 1
                        elif (single_subject_marks_pct_goal == 90) & (score > 65):
                            y_pred = 1
                        else:
                            y_pred = 0
                    else:
                        query = f'SELECT created_on AS test_date,difficulty_level,' \
                                f'SUM(attempt_status="Correct" OR attempt_status="Incorrect") AS attempt_cnt,' \
                                f'SUM(attempt_status="Incorrect") AS attempt_incorrect_cnt,' \
                                f'SUM(attempt_status="Correct") AS attempt_correct,' \
                                f'SUM(gain_marks) AS marksgained,' \
                                f'SUM(question_marks) AS testmarks ' \
                                f'FROM student_questions_attempted a,question_bank_master b ' \
                                f'WHERE a.question_id = b.question_id AND a.student_id = {student_id} AND a.subject_id = {subject} GROUP BY test_date, difficulty_level'

                        df_int = await conn.execute_query_dict(query)
                        df_int = pd.DataFrame(df_int)

                        df_int['difficulty_weights'] = df_int['difficulty_level'].map(difficuly_weights)
                        df_int['weight_marks'] = df_int['marksgained'] * df_int['difficulty_weights']
                        df = df_int.groupby(['test_date']).agg(
                            {'weight_marks': np.sum, 'testmarks': np.sum}).reset_index()
                        df['marks_percentage'] = (df['weight_marks'] / df['testmarks']) * 100
                        df['shifted_marks_percentage'] = df['marks_percentage'].shift(1, axis=0)
                        df['difference'] = df['marks_percentage'] - df['shifted_marks_percentage']
                        df_input = df.sort_values(by='test_date', ascending=True).tail(10)
                        column_names = ['lag_' + str(i) for i in range(0, 10, 1)]
                        df_input = df.sort_values(by='test_date', ascending=True).tail(10).reset_index(drop=True)
                        intermediate_dictionary = {'lag': column_names, 'score': df_input['marks_percentage']}
                        df_lag = pd.DataFrame(intermediate_dictionary)
                        df_pivot = df_lag.pivot_table(columns='lag',
                                                      values=['score'], aggfunc='first')
                        df_pivot.columns.name = None
                        df_pivot.index = pd.RangeIndex(len(df_pivot.index))
                        df_pred_input = df_pivot.copy()
                        if single_subject_marks_pct_goal == 70:
                            # load the model from disk
                            pkl_filename = path_pickle + 'goal_setting_70.pkl'
                            with open(pkl_filename, 'rb') as file:
                                model = pickle.load(file)
                            y_pred = model.predict(df_pred_input)

                        elif single_subject_marks_pct_goal == 80:
                            # load the model from disk
                            pkl_filename = path_pickle + 'goal_setting_80.pkl'
                            with open(pkl_filename, 'rb') as file:
                                model = pickle.load(file)
                            y_pred = model.predict(df_pred_input)

                        elif single_subject_marks_pct_goal == 90:
                            # load the model from disk
                            pkl_filename = path_pickle + 'goal_setting_90.pkl'
                            with open(pkl_filename, 'rb') as file:
                                model = pickle.load(file)
                            y_pred = model.predict(df_pred_input)
                    predictions.append(y_pred)
                    print(y_pred)
                    subjects.append(subject)
                except Exception as e:
                    print(e)
                    continue

            df_predicted = pd.DataFrame(list(zip(subjects, predictions)), columns=['subject', 'reach_goal'])
            # print(df_predicted)
            df_predicted["student_id"] = student_id
            df_predicted = df_predicted.loc[1:, :]
        else:
            df_predicted = pd.DataFrame({'student_id': student_id, 'reach_goal': "null"}, index=[0])
        reach_goal_mapper = {0: 'Inconsistent will Not reach Goal', 1: "Consistent will reach Goal",
                             "null": "Data insufficient"}
        df_predicted['reach_goal'] = df_predicted['reach_goal'].map(reach_goal_mapper)
        print("Time took for execution for this API: %s seconds " % (time.time() - start_time))
        return JSONResponse(status_code=200,content={"response":df_predicted.to_json(orient="records"),"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}', "success": False})

