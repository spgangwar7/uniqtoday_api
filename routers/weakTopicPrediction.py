from datetime import datetime,time
from http import HTTPStatus
from typing import List
import pandas as pd
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import IntegrityError
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
import json
from pandas.io.json import json_normalize
import pickle
from schemas.WeakTopicPrediction import WeakTopic

router = APIRouter(
    prefix='/api',
    tags=['NEET Weak Topic Prediction'],
)

path_pickle = 'pickle/NeetTopicWise/'
path_pickleJEE = 'pickle/JEETopicWise/'


def weights_topics(df_1):
    df = df_1.T
    df.columns = ['weights']

    topic_weights = float(sum(df['weights']))
    if topic_weights == 0.0:
        topic_weights = 1
    target = float(1)
    df['manupulated_data'] = df['weights'] * (target / topic_weights)
    df = df.T
    return df.iloc[1:, :]


def test_grade(numbers,min_score):
    numbers = float(numbers)
    grade=[]
    if numbers < min_score:
        grade = 'Strong'
    else:
        grade = 'Weak'
    return grade

def weaktopics_response(final_prob,cols,col_null):
    list_grade = []
    lstF=[]
    na_cols=[]
    if len(col_null)!=0:
        na_cols = [s.replace('_class', '') for s in col_null]
        final_prob[na_cols] = 999
    testing_code = cols.copy()
    testing_code = [x for x in testing_code if x not in na_cols]
    min_score = 100/len(testing_code)
    for index,row in final_prob.iterrows():
        for i in cols:
            d={}
            val=row[i]*100
            val=format(val, '.2f')
            if(eval(val)<101.00):
                d.update({i:val})
                d.update({'test_grade':test_grade(val,min_score)})
            elif (eval(val) ==99900.00):
                d.update({i:'null'})
                d.update({'test_grade':'unknown'})
            list_grade.append(d)
        lstF.append({"wtopics"+"_student"+str(index) :list_grade})
        return lstF

@router.post('/getNEETWeakTopicPrediction', description='Get NEET Weak Topic Prediction', status_code=201)
def getNEETWeakTopicPrediction(weakTopic:WeakTopic):
    getJson = jsonable_encoder(weakTopic)
    df_j = pd.DataFrame([getJson])

    finalDict = {}
    d = {}
    subject=weakTopic.subject
    #getJson = request.get_json()
    #print("getJson:", getJson)

    # ===============Physics================================
    if subject == "physics" :
        j = getJson["topics"]
        df_j = pd.DataFrame()
        df_j = pd.json_normalize(j)
        gender_encoder = {"F": 1, "M": 0}
        df_j["gender"].replace(gender_encoder, inplace=True)

        with open(path_pickle + 'neetquestionsphysics.pkl', 'rb') as file:
            ratio_input_data_physics = pickle.load(file)

        with open(path_pickle + 'neet_model_input_physics.pkl', 'rb') as file:
            neetmodel_input_data_physics = pickle.load(file)

        with open(path_pickle + 'neet_model_output_class_physics.pkl', 'rb') as file:
            neet_output_class_physics = pickle.load(file)

        df_j = df_j.replace('null', 0)

        for topics in ratio_input_data_physics:
            df_j[topics + '_ratio'] = df_j[topics + '_QAC'] / df_j[topics]

        df_j.fillna(999, inplace=True)
        df_j = df_j[neetmodel_input_data_physics]

        probability_df = pd.DataFrame()
        for category in neet_output_class_physics:
            pkl_filename = path_pickle + 'Neet_physics_Topic_' + category + '.pkl'
            with open(pkl_filename, 'rb') as file:
                model_pickle = pickle.load(file)
            predictions = model_pickle.predict_proba(df_j)
            probability_df[category] = [item[1] for item in predictions]

            # -remove null probabilities---------

        remove_cols = (df_j == 999).any()[lambda x: x].index
        col_null = [s.replace('_ratio', '_class') for s in remove_cols]

        probability_df[col_null] = 0
        cols = [s.replace('_class', '') for s in neet_output_class_physics]
        probability_df.columns = [s.replace('_class', '') for s in neet_output_class_physics]

        final_prob = weights_topics(probability_df)
        final_prob.reset_index(drop=True, inplace=True)
        final_prob.fillna(0, inplace=True)
        # -----------------------------------
        #print(col_null)
        finalDict.update({"physics": weaktopics_response(final_prob, cols, col_null)})

    # ===============Chemistry================================
    if subject == "chemistry" :
        j = getJson["topics"]
        df_j = pd.DataFrame()
        df_j = json_normalize(j)
        gender_encoder = {"F": 1, "M": 0}
        df_j["gender"].replace(gender_encoder, inplace=True)

        with open(path_pickle + 'neetquestionschemistry.pkl', 'rb') as file:
            ratio_input_data_chemistry = pickle.load(file)

        with open(path_pickle + 'neet_model_input_chemistry.pkl', 'rb') as file:
            neetmodel_input_data_chemistry = pickle.load(file)

        with open(path_pickle + 'neet_model_output_class_chemistry.pkl', 'rb') as file:
            neetoutput_class_chemistry = pickle.load(file)

        df_j = df_j.replace('null', 0)

        for topics in ratio_input_data_chemistry:
            df_j[topics + '_ratio'] = df_j[topics + '_QAC'] / df_j[topics]

        df_j.fillna(999, inplace=True)
        df_j = df_j[neetmodel_input_data_chemistry]

        probability_df = pd.DataFrame()
        for category in neetoutput_class_chemistry:
            pkl_filename = path_pickle + 'Neet_chemistry_Topic_' + category + '.pkl'
            with open(pkl_filename, 'rb') as file:
                model_pickle = pickle.load(file)
            predictions = model_pickle.predict_proba(df_j)
            probability_df[category] = [item[1] for item in predictions]

            # -remove null probabilities---------

        remove_cols = (df_j == 999).any()[lambda x: x].index
        col_null = [s.replace('_ratio', '_class') for s in remove_cols]

        probability_df[col_null] = 0
        cols = [s.replace('_class', '') for s in neetoutput_class_chemistry]
        probability_df.columns = [s.replace('_class', '') for s in neetoutput_class_chemistry]

        final_prob = weights_topics(probability_df)
        final_prob.reset_index(drop=True, inplace=True)
        final_prob.fillna(0, inplace=True)

        # -----------------------------------

        finalDict.update({"chemistry": weaktopics_response(final_prob, cols, col_null)})

    # ===============biology================================
    if subject == "biology" :
        j = getJson["biology"]
        df_j = pd.DataFrame()
        df_j = json_normalize(j)
        gender_encoder = {"F": 1, "M": 0}
        df_j["gender"].replace(gender_encoder, inplace=True)

        with open(path_pickle + 'neetquestionsbiology.pkl', 'rb') as file:
            ratio_input_data_bio = pickle.load(file)

        with open(path_pickle + 'neet_model_input_biology.pkl', 'rb') as file:
            model_input_data_bio = pickle.load(file)

        with open(path_pickle + 'neet_model_output_class_biology.pkl', 'rb') as file:
            neetoutput_class_bio = pickle.load(file)

        df_j = df_j.replace('null', 0)

        for topics in ratio_input_data_bio:
            df_j[topics + '_ratio'] = df_j[topics + '_QAC'] / df_j[topics]

        df_j.fillna(999, inplace=True)
        df_j = df_j[model_input_data_bio]
        probability_df = pd.DataFrame()
        for category in neetoutput_class_bio:
            pkl_filename = path_pickle + 'Neet_biology_Topic_' + category + '.pkl'
            with open(pkl_filename, 'rb') as file:
                model_pickle = pickle.load(file)
            predictions = model_pickle.predict_proba(df_j)
            probability_df[category] = [item[1] for item in predictions]

            # -remove null probabilities---------

        remove_cols = (df_j == 999).any()[lambda x: x].index
        col_null = [s.replace('_ratio', '_class') for s in remove_cols]
        probability_df[col_null] = 0

        cols = [s.replace('_class', '') for s in neetoutput_class_bio]
        probability_df.columns = [s.replace('_class', '') for s in neetoutput_class_bio]

        final_prob = weights_topics(probability_df)
        final_prob.reset_index(drop=True, inplace=True)
        final_prob.fillna(0, inplace=True)

        # -----------------------------------

        finalDict.update({"biology": weaktopics_response(final_prob, cols, col_null)})

    return json.dumps(finalDict)

@router.post('/getJEEWeakTopicPrediction', description='Get JEE Weak Topic Prediction', status_code=201)
def getJEEWeakTopicPrediction(weakTopic:WeakTopic):
    getJson = jsonable_encoder(weakTopic)
    df_j = pd.DataFrame([getJson])


    finalDict = {}
    d = {}
    subject=weakTopic.subject
    #getJson = request.get_json()
    #print("getJson:", getJson)

    # ===============Physics================================


    if subject == "physics" :

        j = getJson["physics"]
        df_j = pd.DataFrame()
        df_j = pd.json_normalize(j)
        gender_encoder = {"F": 1, "M": 0}
        df_j["gender"].replace(gender_encoder, inplace=True)

        with open(path_pickleJEE + 'questionsphysics.pkl', 'rb') as file:
            ratio_input_data_physics = pickle.load(file)

        with open(path_pickleJEE + 'model_input_physics.pkl', 'rb') as file:
            model_input_data_physics = pickle.load(file)

        with open(path_pickleJEE + 'model_output_class_physics.pkl', 'rb') as file:
            output_class = pickle.load(file)

        df_j = df_j.replace('null', 0)

        for topics in ratio_input_data_physics:
            df_j[topics + '_ratio'] = df_j[topics + '_QAC'] / df_j[topics]

        df_j.fillna(999, inplace=True)
        df_j = df_j[model_input_data_physics]
        probability_df = pd.DataFrame()
        for category in output_class:
            pkl_filename = path_pickleJEE + 'JeeTopic_' + category + '.pkl'
            with open(pkl_filename, 'rb') as file:
                model_pickle = pickle.load(file)
            predictions = model_pickle.predict_proba(df_j)
            probability_df[category] = [item[1] for item in predictions]

            # -remove null probabilities---------

        remove_cols = (df_j == 999).any()[lambda x: x].index
        col_null = [s.replace('_ratio', '_class') for s in remove_cols]

        probability_df[col_null] = 0
        cols = [s.replace('_class', '') for s in output_class]
        probability_df.columns = [s.replace('_class', '') for s in output_class]

        final_prob = weights_topics(probability_df)
        final_prob.reset_index(drop=True, inplace=True)
        final_prob.fillna(0, inplace=True)
        # -----------------------------------

        finalDict.update({"physics": weaktopics_response(final_prob, cols, col_null)})
    # ===============Chemistry================================
    if subject == "chemistry":

        j = getJson["chemistry"]
        df_j = json_normalize(j)
        gender_encoder = {"F": 1, "M": 0}
        df_j["gender"].replace(gender_encoder, inplace=True)

        with open(path_pickleJEE + 'questionschemistry.pkl', 'rb') as file:
            ratio_input_data_chemistry = pickle.load(file)

        with open(path_pickleJEE + 'model_input_chemistry.pkl', 'rb') as file:
            model_input_data_chemistry = pickle.load(file)

        with open(path_pickleJEE + 'model_output_class_chemistry.pkl', 'rb') as file:
            output_class_chemistry = pickle.load(file)

        df_j = df_j.replace('null', 0)

        for topics in ratio_input_data_chemistry:
            df_j[topics + '_ratio'] = df_j[topics + '_QAC'] / df_j[topics]

        df_j.fillna(999, inplace=True)
        df_j = df_j[model_input_data_chemistry]
        probability_df = pd.DataFrame()
        for category in output_class_chemistry:
            pkl_filename = path_pickleJEE + 'JeeTopic_' + category + '.pkl'
            with open(pkl_filename, 'rb') as file:
                model_pickle = pickle.load(file)
            predictions = model_pickle.predict_proba(df_j)
            probability_df[category] = [item[1] for item in predictions]

            # -remove null probabilities---------

        remove_cols = (df_j == 999).any()[lambda x: x].index
        col_null = [s.replace('_ratio', '_class') for s in remove_cols]

        probability_df[col_null] = 0
        cols = [s.replace('_class', '') for s in output_class_chemistry]
        probability_df.columns = [s.replace('_class', '') for s in output_class_chemistry]

        final_prob = weights_topics(probability_df)
        final_prob.reset_index(drop=True, inplace=True)
        final_prob.fillna(0, inplace=True)

        # -----------------------------------
        finalDict.update({"chemistry": weaktopics_response(final_prob, cols, col_null)})
    # ===============Mathematics================================
    if subject == "mathematics":
        j = getJson["mathematics"]
        df_j = json_normalize(j)
        gender_encoder = {"F": 1, "M": 0}
        df_j["gender"].replace(gender_encoder, inplace=True)

        with open(path_pickleJEE + 'questionsmaths.pkl', 'rb') as file:
            ratio_input_data_maths = pickle.load(file)

        with open(path_pickleJEE + 'model_input_maths.pkl', 'rb') as file:
            model_input_data_maths = pickle.load(file)

        with open(path_pickleJEE + 'model_output_class_maths.pkl', 'rb') as file:
            output_class_maths = pickle.load(file)

        df_j = df_j.replace('null', 0)

        for topics in ratio_input_data_maths:
            df_j[topics + '_ratio'] = df_j[topics + '_QAC'] / df_j[topics]

        df_j.fillna(999, inplace=True)
        df_j = df_j[model_input_data_maths]
        probability_df = pd.DataFrame()
        for category in output_class_maths:
            pkl_filename = path_pickleJEE + 'JeeTopic_' + category + '.pkl'
            with open(pkl_filename, 'rb') as file:
                model_pickle = pickle.load(file)
            predictions = model_pickle.predict_proba(df_j)
            probability_df[category] = [item[1] for item in predictions]

            # -remove null probabilities---------

        remove_cols = (df_j == 999).any()[lambda x: x].index
        col_null = [s.replace('_ratio', '_class') for s in remove_cols]
        probability_df[col_null] = 0

        cols = [s.replace('_class', '') for s in output_class_maths]
        probability_df.columns = [s.replace('_class', '') for s in output_class_maths]

        final_prob = weights_topics(probability_df)
        final_prob.reset_index(drop=True, inplace=True)
        final_prob.fillna(0, inplace=True)

        # -----------------------------------
        finalDict.update({"mathematics": weaktopics_response(final_prob, cols, col_null)})
    return json.dumps(finalDict)
