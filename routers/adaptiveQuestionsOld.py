import traceback
from http import HTTPStatus
import numpy as np
import pandas as pd
from tortoise import Tortoise
from tortoise.queryset import QuerySet
from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import *
from tortoise.query_utils import Q
from fastapi.responses import JSONResponse
from schemas.AdaptiveQuestions import adaptiveQuestions
import pickle
from sklearn.preprocessing import LabelEncoder
import time

import json

router = APIRouter(
    prefix='/api',
    tags=['Adaptive Questions'],
)

path_pickle = 'pickle/'


@router.post('/get-adaptive-questions', description="get adaptive questions based on student's past performnace")
async def getAdaptiveQuestions(data: adaptiveQuestions):
    conn = Tortoise.get_connection('default')

    single_exam_id = data.exam_id
    single_student_id = data.student_id
    single_total_questions = data.total_questions
    query = f'Select question_bank_name,time_allowed,questions_cnt,exam_paper_single_mult_flag from class_exams where id ={single_exam_id}'
    df_quiz = await conn.execute_query_dict(query)
    df_quiz = pd.DataFrame(df_quiz)
    df_quiz = df_quiz.reset_index(drop=True).iloc[0]
    quiz_bank = df_quiz['question_bank_name']
    query = f'select student_id,language_id from student_preferences where student_id = {single_student_id}'
    language = await conn.execute_query_dict(query)
    language = pd.DataFrame(language)
    # print(language)
    language = language.reset_index(drop=True).iloc[0]
    single_language = language['language_id']

    # take for testing
    single_language = 1

    # mutli subject /single exam pattern finder,get all the subjects for which student is applied on platform
    if df_quiz['exam_paper_single_mult_flag'] == "M":
        print('checking mulitple streams')
        query = f'select student_id,subject_id,exam_id,paper_number from student_subjects where student_id = {single_student_id}'
        subject_list_df = await conn.execute_query_dict(query)
        subject_list_df = pd.DataFrame(subject_list_df)
        subject_list_df = subject_list_df.reset_index(drop=True)
        subject_list_df = subject_list_df[subject_list_df['subject_id'].notna()]
        if subject_list_df.shape[0] == 1:
            subject_list = "(" + str(list(subject_list_df['subject_id'].value_counts().index)[0]) + ")"
        else:
            subject_list = tuple(list(subject_list_df['subject_id'].value_counts().index))
    else:
        query = f'Select class_exam_id,subject_id,weightage_pct,subj_question_cnt from exam_subject_chapters where class_exam_id = {single_exam_id}'
        total_questions = await conn.execute_query_dict(query)
        total_questions = pd.DataFrame(total_questions)
        subject_list_df = total_questions[total_questions['subject_id'].notna()]
        if subject_list_df.shape[0] == 1:
            subject_list = "(" + str(list(subject_list_df['subject_id'].value_counts().index)[0]) + ")"
        else:
            subject_list = tuple(list(subject_list_df['subject_id'].value_counts().index))
    # print(subject_list)
    # print(quiz_bank)
    query = f'SELECT question_id,class_id,subject_id,topic_id,difficulty_level,skill_id,system_driven_complexity,answered_correctly_pct FROM  {quiz_bank} where subject_id IN {subject_list} and language_id = {single_language}'
    val = await conn.execute_query_dict(query)
    df = pd.DataFrame(val)

    df.fillna(0, inplace=True)
    # print(df)
    # print((df['question_id']==2054)==True)
    cols = ['subject_id', 'difficulty_level', 'skill_id']  # Set columns to combine
    df['content_id'] = df[cols].apply(lambda row: '_ '.join(row.values.astype(str)), axis=1)
    df = df[['class_id', 'question_id', 'subject_id', 'topic_id', 'content_id', 'answered_correctly_pct']]

    query = f"SELECT sqa.student_id,sqa.subject_id,sr.test_type AS exam_type,sqa.question_id," \
            f"SUM(sqa.attempt_status='Correct' OR sqa.attempt_status='Incorrect') AS attempt_cnt," \
            f"SUM(sqa.attempt_status='Correct') AS attempt_correct," \
            f"SUM(sqa.attempt_status='Incorrect') AS attempt_incorrect_cnt," \
            f"SUM(sqa.attempt_status='Unanswered') AS unattempt_cnt,sqa.question_marks,sqa.gain_marks," \
            f"sqa.time_taken as time_taken_sec FROM student_questions_attempted sqa JOIN student_results sr ON sqa.student_id=sr.user_id WHERE student_id={single_student_id} and class_exam_id={single_exam_id}"
    student_attempts = await conn.execute_query_dict(query)

    student_attempts = pd.DataFrame(student_attempts)
    print(student_attempts['student_id'].iloc[0])
    if student_attempts['student_id'].iloc[0] == None:
        return JSONResponse(status_code=400, content=json.dumps({"student_id": str(single_student_id),
                                                                 "message": "Student haven't interacted with our system yet No data available in DB"}))
    print(student_attempts)

    student_attempts = student_attempts[['student_id', 'subject_id',
                                         'exam_type', 'question_id', 'attempt_cnt',
                                         'attempt_correct', 'attempt_incorrect_cnt', 'unattempt_cnt',
                                         'question_marks', 'gain_marks', 'time_taken_sec']].reset_index(drop=True)

    # students overall score in past

    if student_attempts.empty:

        return JSONResponse(status_code=400, content=json.dumps({"student_id": str(single_student_id),
                                                                 "message": "Student haven't interacted with our system yet No data available in DB"}))

    else:
        print(student_attempts.dtypes)
        student_attempts['attempt_correct']=student_attempts['attempt_correct'].astype(int)
        student_attempts['attempt_cnt']=student_attempts['attempt_cnt'].astype(int)

        previous_marks = student_attempts.groupby(['student_id', 'subject_id']).agg({'attempt_correct': ["sum", "mean"], 'attempt_cnt': 'count'}).reset_index()

        previous_marks.columns = ['student_id', 'subject_id', 'correct_attempted_sum', 'correct_attempted_mean',
                                  'sum_attempt_cnt']
        student_perf = student_attempts.merge(previous_marks, on=('student_id', 'subject_id'), how='left')
        student_perf.drop(['subject_id'], axis=1, inplace=True)
    # print(student_attempts)
    print(previous_marks)
    # print(student_perf)
    print(previous_marks['correct_attempted_sum'])
    train_df = df.merge(student_perf, on='question_id', how='left')
    filt = (train_df['time_taken_sec'] == 'NaT')
    train_df.loc[filt, 'time_taken_sec'] = "00:00:00"

    preproc_df = train_df[['class_id', 'student_id', 'subject_id', 'topic_id',
                           'question_id', 'time_taken_sec', 'correct_attempted_sum',
                           'correct_attempted_mean', 'sum_attempt_cnt', 'content_id',
                           'answered_correctly_pct', 'attempt_correct']]

    preproc_df['time_taken_sec'] = train_df['time_taken_sec'] / np.timedelta64(1, 's')
    preproc_df = preproc_df.fillna(0)
    preproc_df['correct_attempted_sum'] = preproc_df['correct_attempted_sum'].astype(int)

    # preproc_df['correct_attempted_sum'].astype(float)

    print(preproc_df.head(10))
    if single_exam_id == 1:
        pkl_file = open(f'{path_pickle}content_encoder_jee.pkl', 'rb')
        enc = pickle.load(pkl_file)
        pkl_filename = path_pickle + 'adaptive_model_jee.pkl'
        with open(pkl_filename, 'rb') as file:
            model = pickle.load(file)
    elif single_exam_id == 2:
        pkl_file = open(f'{path_pickle}content_encoder_neet.pkl', 'rb')
        enc = pickle.load(pkl_file)
        pkl_filename = path_pickle + 'adaptive_model_neet.pkl'
        with open(pkl_filename, 'rb') as file:
            model = pickle.load(file)
    elif single_exam_id == 3:
        pkl_file = open(f'{path_pickle}content_encoder_clat.pkl', 'rb')
        enc = pickle.load(pkl_file)
        pkl_filename = path_pickle + 'adaptive_model_clat.pkl'
        with open(pkl_filename, 'rb') as file:
            model = pickle.load(file)
    elif single_exam_id == 4:
        pkl_file = open(f'{path_pickle}content_encoder_10.pkl', 'rb')
        enc = pickle.load(pkl_file)
        pkl_filename = path_pickle + 'adaptive_model_10.pkl'
        with open(pkl_filename, 'rb') as file:
            model = pickle.load(file)
    elif single_exam_id == 5:
        pkl_file = open(f'{path_pickle}content_encoder_12.pkl', 'rb')
        enc = pickle.load(pkl_file)
        pkl_filename = path_pickle + 'adaptive_model_12.pkl'
        with open(pkl_filename, 'rb') as file:
            model = pickle.load(file)
    elif single_exam_id == 6:
        pkl_file = open(f'{path_pickle}content_encoder_11.pkl', 'rb')
        enc = pickle.load(pkl_file)
        pkl_filename = path_pickle + 'adaptive_model_11.pkl'
        with open(pkl_filename, 'rb') as file:
            model = pickle.load(file)
    elif single_exam_id == 7:
        pkl_file = open(f'{path_pickle}content_encoder_9.pkl', 'rb')
        enc = pickle.load(pkl_file)
        pkl_filename = path_pickle + 'adaptive_model_9.pkl'
        with open(pkl_filename, 'rb') as file:
            model = pickle.load(file)
    elif single_exam_id == 14:
        pkl_file = open(f'{path_pickle}content_encoder_cat.pkl', 'rb')
        enc = pickle.load(pkl_file)
        pkl_filename = path_pickle + 'adaptive_model_cat.pkl'
        with open(pkl_filename, 'rb') as file:
            model = pickle.load(file)
    enc = LabelEncoder().fit(preproc_df['content_id'].astype(str))
    preproc_df['content_id'] = enc.transform(preproc_df['content_id'].astype(str))
    preproc_df['student_id'] = single_student_id
    preproc_df = preproc_df[['class_id', 'student_id', 'subject_id', 'topic_id', 'question_id',
                             'time_taken_sec', 'correct_attempted_sum', 'correct_attempted_mean',
                             'sum_attempt_cnt', 'content_id', 'answered_correctly_pct']]
    preproc_df = preproc_df.drop_duplicates(subset='question_id', keep='first')

    preproc_df.info()
    y_pred = model.predict(preproc_df)
    proba_question_id = dict(zip(list(preproc_df['question_id']), list(y_pred)))
    final_question = {k: v for k, v in sorted(proba_question_id.items(), key=lambda item: item[1], reverse=True)}
    ignore_questions = list(
        student_attempts[student_attempts['attempt_correct'] == 1]['question_id'].reset_index(drop=True))
    # get unique unattempted questions studentwise
    if len(ignore_questions) != df.shape[0]:
        ignore_questions = set(ignore_questions).intersection(set(df['question_id']))
        for key in ignore_questions:
            final_question.pop(key, 'No Key found')
    adaptive_questions = list(final_question.keys())[:single_total_questions]

    return JSONResponse(status_code=200, content=json.dumps(
        {"student_id": str(single_student_id), "Question_id": str(adaptive_questions)}))


@router.post('/get-adaptive-questions-practice', description="get adaptive questions based on student's past performnace")
async def getAdaptiveQuestionsPractice(data: adaptiveQuestions):
    conn = Tortoise.get_connection('default')

    single_exam_id = data.exam_id
    single_student_id = data.student_id
    single_total_questions = data.total_questions
    query = f'Select question_bank_name,time_allowed,questions_cnt,exam_paper_single_mult_flag from class_exams where id ={single_exam_id}'
    df_quiz = await conn.execute_query_dict(query)
    df_quiz = pd.DataFrame(df_quiz)
    df_quiz = df_quiz.reset_index(drop=True).iloc[0]
    quiz_bank = df_quiz['question_bank_name']
    query = f'select student_id,language_id from student_preferences where student_id = {single_student_id}'
    language = await conn.execute_query_dict(query)
    language = pd.DataFrame(language)
    # print(language)
    language = language.reset_index(drop=True).iloc[0]
    single_language = language['language_id']

    # take for testing
    single_language = 1

    # mutli subject /single exam pattern finder,get all the subjects for which student is applied on platform
    if df_quiz['exam_paper_single_mult_flag'] == "M":
        print('checking mulitple streams')
        query = f'select student_id,subject_id,exam_id,paper_number from student_subjects where student_id = {single_student_id}'
        subject_list_df = await conn.execute_query_dict(query)
        subject_list_df = pd.DataFrame(subject_list_df)
        subject_list_df = subject_list_df.reset_index(drop=True)
        subject_list_df = subject_list_df[subject_list_df['subject_id'].notna()]
        if subject_list_df.shape[0] == 1:
            subject_list = "(" + str(list(subject_list_df['subject_id'].value_counts().index)[0]) + ")"
        else:
            subject_list = tuple(list(subject_list_df['subject_id'].value_counts().index))
    else:
        query = f'Select class_exam_id,subject_id,weightage_pct,subj_question_cnt from exam_subject_chapters where class_exam_id = {single_exam_id}'
        total_questions = await conn.execute_query_dict(query)
        total_questions = pd.DataFrame(total_questions)
        subject_list_df = total_questions[total_questions['subject_id'].notna()]
        if subject_list_df.shape[0] == 1:
            subject_list = "(" + str(list(subject_list_df['subject_id'].value_counts().index)[0]) + ")"
        else:
            subject_list = tuple(list(subject_list_df['subject_id'].value_counts().index))
    #print(subject_list)
    # print(quiz_bank)
    query = f'SELECT question_id,class_id,subject_id,topic_id,difficulty_level,skill_id,system_driven_complexity,answered_correctly_pct FROM  {quiz_bank} where subject_id IN {subject_list} and language_id = {single_language}'
    query=f'SELECT qbj.question_id,qbj.class_id,qbj.subject_id,qbj.topic_id,qbj.difficulty_level,skill_id,qdl.concept_level,system_driven_complexity,answered_correctly_pct FROM {quiz_bank} as qbj inner join question_difficulty_levels as qdl on qbj.difficulty_level=qdl.id where qbj.subject_id IN {subject_list} and qbj.language_id = {single_language}'
    val = await conn.execute_query_dict(query)
    df = pd.DataFrame(val)
    filt1 = (df['concept_level'] == 'L')
    df.loc[filt1, 'difficulty_level'] = 0
    filt2 = (df['concept_level'] == 'M')
    df.loc[filt2, 'difficulty_level'] = 1
    filt3 = (df['concept_level'] == 'H')
    df.loc[filt3, 'difficulty_level'] = 2
    df.fillna(0, inplace=True)
    print(df)
    # print((df['question_id']==2054)==True)
    cols = ['subject_id', 'difficulty_level', 'skill_id']  # Set columns to combine
    df['content_id'] = df[cols].apply(lambda row: '_ '.join(row.values.astype(str)), axis=1)
    df = df[['class_id', 'question_id', 'subject_id', 'topic_id', 'difficulty_level','skill_id','system_driven_complexity', 'answered_correctly_pct']]
