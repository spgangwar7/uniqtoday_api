import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from tortoise import Tortoise
from tortoise.queryset import QuerySet
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from fastapi.responses import JSONResponse
from schemas.AdaptiveQuestions import questionPrediction
import pickle
import re
import joblib
router = APIRouter(
    prefix='/api/adaptive',
    tags=['Prediction'],
)

@router.post('/export-question-bank',description='Export Question Bank')
async def exportQuestionBank():
    try:
        conn = Tortoise.get_connection('default')
        #Export JEE Question Bank as csv and create a apriori model file

        jee_question_bank_query = "SELECT qbj.question_id,qbj.subject_id,qbj.topic_id,qbj.difficulty_level,skill_id,qdl.concept_level FROM question_bank_jee as qbj inner join question_difficulty_levels as qdl on qbj.difficulty_level=qdl.id;"
        jee_question_bank = await conn.execute_query_dict(jee_question_bank_query)
        jee_question_bank_df = pd.DataFrame(jee_question_bank)
        jee_question_bank_df=jee_question_bank_df.fillna(0)
        filt1 = (jee_question_bank_df['concept_level'] == 'L')
        jee_question_bank_df.loc[filt1, 'difficulty_level'] = 0
        filt2 = (jee_question_bank_df['concept_level'] == 'M')
        jee_question_bank_df.loc[filt2, 'difficulty_level'] = 1
        filt3 = (jee_question_bank_df['concept_level'] == 'H')
        jee_question_bank_df.loc[filt3, 'difficulty_level'] = 2

        jee_question_bank_df.to_csv("question_bank_jee.csv")
        df = jee_question_bank_df[['difficulty_level', 'subject_id', 'topic_id', 'skill_id']].astype(int)
        dfgrouponehot = pd.get_dummies(df, columns=['difficulty_level', 'subject_id', 'topic_id', 'skill_id'],
                                       prefix=['difficulty_level', 'subject_id', 'topic_id', 'skill_id'])
        frequent_itemsets = apriori(dfgrouponehot, min_support=0.001, use_colnames=True)
        rules = association_rules(frequent_itemsets, metric="lift", min_threshold=0.001)
        joblib.dump(rules, "jee_model.cls")

        #Export NEET Question Bank as csv and create a apriori model file

        neet_question_bank_query = "SELECT qbn.question_id,qbn.subject_id,qbn.topic_id,qbn.difficulty_level,skill_id,qdl.concept_level FROM question_bank_neet as qbn inner join question_difficulty_levels as qdl on qbn.difficulty_level=qdl.id;"
        neet_question_bank = await conn.execute_query_dict(neet_question_bank_query)
        neet_question_bank_df = pd.DataFrame(neet_question_bank)
        neet_question_bank_df = neet_question_bank_df.fillna(0)
        filt1 = (neet_question_bank_df['concept_level'] == 'L')
        neet_question_bank_df.loc[filt1, 'difficulty_level'] = 0
        filt2 = (neet_question_bank_df['concept_level'] == 'M')
        neet_question_bank_df.loc[filt2, 'difficulty_level'] = 1
        filt3 = (neet_question_bank_df['concept_level'] == 'H')
        neet_question_bank_df.loc[filt3, 'difficulty_level'] = 2
        neet_question_bank_df.to_csv("question_bank_neet.csv")
        df = neet_question_bank_df[['difficulty_level', 'subject_id', 'topic_id', 'skill_id']].astype(int)
        dfgrouponehot = pd.get_dummies(df, columns=['difficulty_level', 'subject_id', 'topic_id', 'skill_id'],
                                       prefix=['difficulty_level', 'subject_id', 'topic_id', 'skill_id'])
        frequent_itemsets = apriori(dfgrouponehot, min_support=0.001, use_colnames=True)
        rules = association_rules(frequent_itemsets, metric="lift", min_threshold=0.001)
        joblib.dump(rules, "neet_model.cls")
        resp = {
            "message": "Model Created Successfully",
            "success": True
        }
        return resp
    except Exception as e:
        print(e)
        resp = {
            "message": "Model was not created due to error",
            "success": False
        }
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content=resp)

@router.post('/predict-questions',description='predict new questions based on attempt status of old questions')
async def PredictQuestions(data:questionPrediction):
    try:
        conn=Tortoise.get_connection('default')
        data = jsonable_encoder(data)
        unique_questions=data['unique_question_list']
        class_exam_id=data['class_exam_id']
        print(class_exam_id)
        test_df = pd.DataFrame(data)
        #print(test_df)
        if class_exam_id==1:
            df=pd.read_csv("question_bank_jee.csv")
            rules = joblib.load("jee_model.cls")
        elif class_exam_id==2:
            df = pd.read_csv("question_bank_neet.csv")
            rules = joblib.load("neet_model.cls")
        questions_list=[]
        if not unique_questions:
            unique_questions_list=[]
        else:
            unique_questions_list=unique_questions.split(",")
        print(len(test_df))
        if test_df.empty:
            if class_exam_id == 1:
                query = f'select question_id from question_bank_jee where class_id={class_exam_id} limit 2'
            elif class_exam_id == 2:
                query = f'select question_id from question_bank_neet where class_id={class_exam_id} limit 2'
            result = await conn.execute_query_dict(query)
            for question in result:
                questions_list.append(question['question_id'])
                unique_questions_list.append(question['question_id'])
        for i in range(len(test_df)):
            question_id=test_df['question_id'].loc[i]
            print(question_id)
            unique_questions_list.append(int(question_id))
            input_df=df.loc[df['question_id']==question_id]
            input_df=input_df.reset_index(drop=True)
            #print(input_df)
            difficulty_level=input_df['difficulty_level'].loc[0]
            attempt_status=test_df['attempt_status'].loc[i]
            subject_id = input_df['subject_id'].loc[0]
            topic_id = input_df['topic_id'].loc[0]

            if attempt_status=="Incorrect" or attempt_status=="Unanswered":
                if difficulty_level!=0:
                    difficulty_level=difficulty_level-1
            if attempt_status == "Correct" :
                if difficulty_level!=2 :
                    difficulty_level=difficulty_level+1
            subject_id_str="subject_id_"+str(subject_id)
            topic_id_str="topic_id_"+str(topic_id)
            difficulty_level_str="difficulty_level_"+str(difficulty_level)
            #print("Getting suggestions for "+subject_id_str+", "+topic_id_str+", "+difficulty_level_str)
            suggestions=rules[rules['antecedents'] == {subject_id_str,difficulty_level_str}]
            #print(suggestions)
            antecedents=suggestions['antecedents'].reset_index(drop=True)
            suggestions=suggestions['consequents'].reset_index(drop=True)
            combination=[]
            for x in suggestions.loc[0]:
                combination.append(x)
            for y in antecedents.loc[0]:
                combination.append(y)
            #print(combination)
            d = dict(s.rsplit('_',1) for s in combination)
            #print(d)
            if class_exam_id == 1:
                selectquery = f'select question_id from question_bank_jee where '
            elif class_exam_id == 2:
                selectquery = f'select question_id from question_bank_neet where '
            i=0
            for key,value in d.items():
                if i==0:
                    selectquery = selectquery + f'{key} ="{value}"'
                else:
                    selectquery = selectquery + f' and {key} ="{value}"'
                #print(key,value)
                i+=1
            selectquery=selectquery+" ORDER BY RAND() limit 1 "
            #print(selectquery)
            result=await conn.execute_query_dict(selectquery)
            result=result[0]
            if result in unique_questions_list:
                result = await conn.execute_query_dict(selectquery)
                result = result[0]
                questions_list.append(result['question_id'])
                unique_questions_list.append(result['question_id'])
            else:
                questions_list.append(result['question_id'])
                unique_questions_list.append(result['question_id'])
            print(questions_list)
        unique_questions_list = ','.join(map(str, unique_questions_list))
        resp = {
                "question_id": questions_list,
                "unique_question_list":unique_questions_list,
                "success": True
            }
        return resp
    except Exception as e:
        print(e)
        resp = {
            "message": "Model was not created due to error",
            "success": False
        }
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content=resp)

"""
@router.post('/export-question-bank2',description='Export Question Bank')
async def exportQuestionBank2():
    try:
        conn = Tortoise.get_connection('default')
        #Export JEE Question Bank as csv and create a apriori model file

        jee_question_bank_query = "SELECT question_id,class_exam_cd, subjects.subject_name,esu.uni_name,esc.chapter_name,topics.topic_name,difficulty_level FROM learntoday_uat.question_bank_jee as qbj inner join class_exams on qbj.class_id=class_exams.id inner join subjects on qbj.subject_id = subjects.id inner join exam_subject_units as esu on qbj.unit_id=esu.unit_id inner join exam_subject_chapters as esc on qbj.chapter_id=esc.chapter_id inner join topics on qbj.topic_id=topics.id where qbj.unit_id is not null and qbj.chapter_id is not null and qbj.topic_id is not null;"
        jee_question_bank = await conn.execute_query_dict(jee_question_bank_query)
        jee_question_bank_df = pd.DataFrame(jee_question_bank)
        jee_question_bank_df=jee_question_bank_df.fillna(0)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content="File not created")
"""