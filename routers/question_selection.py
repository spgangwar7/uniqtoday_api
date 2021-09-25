import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
from bs4 import BeautifulSoup
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from db.engine import db_connection
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from schemas.QuestionSelection import Question_Selection
from schemas.QuestionSelection import AdvanceQuestionSelectiontest
from schemas.QuestionSelection import AdvanceQuestionSelectiontest2
from schemas.QuestionSelection import PlannerQuestionSelection
from fastapi import BackgroundTasks, FastAPI
import jwt
import json
import logging
import numpy as np
import itertools
import requests
from datetime import datetime,timedelta
import time
import redis

router = APIRouter(
    prefix='/api',
    tags=['Question Selection'],
)


async def jwtAuth(conn):
    #crsr=connection.cursor()
    token_flag = True
    try:
        token_passed = requests.headers['Authorization']
        token_passed = token_passed.split(' ')[-1]
        token_data = jwt.decode(token_passed, options={"verify_signature":False })
    except Exception as e:
        print("Exception in JWT Header : \n {}".format(e))
        return token_flag
    token_sql = f"SELECT count(*) FROM student_users where id={token_data['user_id']} and jwt_token='{token_passed}'"
    res_token = await conn.execute_query_dict(token_sql)

    token_flag = True
    print(token_flag)
    #if res_token.shape[0] > 0:
    #    token_flag = True
    return token_flag

async def update_db_exhaust_date(student_id):
    conn = Tortoise.get_connection("default")
    date_exhausted = (datetime.now()).strftime("%Y-%m-%d")
    query_update = f'UPDATE student_preferences SET question_bank_exhausted_flag="Yes", ques_exhausted_date="{date_exhausted}" WHERE student_id = {student_id}'
    await conn.execute_query_dict(query_update)
    return (print('updated exhausted date in DB'))


async def get_custom_from_DB2(df_j, subject_id, topic_list,chapter_id,quiz_bank):
    try:
        conn = Tortoise.get_connection("default")
        df = df_j

        student_id_input = df['student_id'].iloc[0]
        single_exam_id = df['exam_id'].iloc[0]
        single_total_question_cnt = int(df['question_cnt'].iloc[0])
        # question bank mapper
        # query = f'select id,language_id from student_users where id = {student_id_input}'

        #print(topic_list)
        try:
            topic_id_list_int = json.loads(topic_list)
        except:
            topic_id_list_int = [0]
        final_question = pd.DataFrame()
        # questions are fetched based on difficulty level,question from source,category,subjects equally divide as per count

        try:
            topic_id_list = [int(i) for i in topic_id_list_int]
            #print(topic_id_list)
            # 1. Get questions which user has not answered before
            if topic_id_list[0] == 0:
                #query when topic list is empty
                #print("Getting unaswered questions")
                if chapter_id == 0:
                    query = f'SELECT b.question_id,b.subject_id,b.topic_id,b.difficulty_level,b.chapter_id as category FROM student_questions_attempted a INNER JOIN {quiz_bank} b ON a.question_id = b.question_id WHERE a.student_id ={student_id_input} and a.class_exam_id = {single_exam_id} and a.subject_id = {subject_id}  and a.attempt_status="Unanswered" ORDER BY RAND() limit {single_total_question_cnt}'
                else:
                    query = f'SELECT b.question_id,b.subject_id,b.topic_id,b.difficulty_level,b.chapter_id as category FROM student_questions_attempted a INNER JOIN {quiz_bank} b ON a.question_id = b.question_id WHERE a.student_id ={student_id_input} and a.class_exam_id = {single_exam_id} and a.subject_id = {subject_id} and a.chapter_id = {chapter_id} and a.attempt_status="Unanswered" ORDER BY RAND() limit {single_total_question_cnt}'
                #query = f'SELECT question_id,subject_id,topic_id,difficulty_level,chapter_id as category FROM question_bank_master where subject_id = {subject_id}   and chapter_id = {chapter_id} ORDER BY RAND() limit {single_total_question_cnt}'
            else:
                topic_id_list = tuple(topic_id_list)
                #query when topic list is not empty
                if chapter_id == 0:
                    query = f'SELECT b.question_id,b.subject_id,b.topic_id,b.difficulty_level,b.chapter_id as category FROM student_questions_attempted a INNER JOIN {quiz_bank} b ON a.question_id = b.question_id WHERE a.student_id ={student_id_input} and a.class_exam_id = {single_exam_id} and a.subject_id = {subject_id} and a.attempt_status="Unanswered"  and a.topic_id IN {topic_id_list} ORDER BY RAND() limit {single_total_question_cnt}'
                else:
                    query = f'SELECT b.question_id,b.subject_id,b.topic_id,b.difficulty_level,b.chapter_id as category FROM student_questions_attempted a INNER JOIN {quiz_bank} b ON a.question_id = b.question_id WHERE a.student_id ={student_id_input} and a.class_exam_id = {single_exam_id} and a.subject_id = {subject_id} and a.chapter_id = {chapter_id} and a.attempt_status="Unanswered"  and a.topic_id IN {topic_id_list} ORDER BY RAND() limit {single_total_question_cnt}'
                #query = f'SELECT question_id,subject_id,topic_id,difficulty_level,chapter_id as category FROM question_bank_master where subject_id = {subject_id}  and chapter_id = {chapter_id} and topic_id IN {topic_id_list} ORDER BY RAND() limit {single_total_question_cnt}'
            #print(query)
            new_df = await conn.execute_query_dict(query)
            new_df=pd.DataFrame(new_df)
            new_df=new_df.reset_index(drop=True)
            final_question = pd.concat([final_question, new_df], axis=0)

            if final_question.shape[0] < single_total_question_cnt:
                # 2: Get questions which user have answered wrong previously
                if topic_id_list[0] == 0:
                    #query when topic list is empty
                    if chapter_id == 0:
                        query = f'SELECT b.question_id,b.subject_id,b.topic_id,b.difficulty_level,b.chapter_id as category FROM student_questions_attempted a INNER JOIN {quiz_bank} b ON a.question_id = b.question_id WHERE a.student_id ={student_id_input} and a.class_exam_id = {single_exam_id} and a.subject_id = {subject_id} and a.attempt_status="Incorrect" ORDER BY RAND() limit {single_total_question_cnt}'
                    else:
                        query = f'SELECT b.question_id,b.subject_id,b.topic_id,b.difficulty_level,b.chapter_id as category FROM student_questions_attempted a INNER JOIN {quiz_bank} b ON a.question_id = b.question_id WHERE a.student_id ={student_id_input} and a.class_exam_id = {single_exam_id} and a.subject_id = {subject_id} and a.chapter_id = {chapter_id} and a.attempt_status="Incorrect" ORDER BY RAND() limit {single_total_question_cnt}'

                    #query = f'SELECT question_id,subject_id,topic_id,difficulty_level,chapter_id as category FROM question_bank_master where subject_id = {subject_id}   and chapter_id = {chapter_id} ORDER BY RAND() limit {single_total_question_cnt}'
                else:
                    #query when topic list is not empty
                    if chapter_id == 0:
                        query = f'SELECT b.question_id,b.subject_id,b.topic_id,b.difficulty_level,b.chapter_id as category FROM student_questions_attempted a INNER JOIN {quiz_bank} b ON a.question_id = b.question_id WHERE a.student_id ={student_id_input} and a.class_exam_id = {single_exam_id} and a.subject_id = {subject_id} and a.attempt_status="Incorrect"  and a.topic_id IN {topic_id_list} ORDER BY RAND() limit {single_total_question_cnt}'
                    else:
                        query = f'SELECT b.question_id,b.subject_id,b.topic_id,b.difficulty_level,b.chapter_id as category FROM student_questions_attempted a INNER JOIN {quiz_bank} b ON a.question_id = b.question_id WHERE a.student_id ={student_id_input} and a.class_exam_id = {single_exam_id} and a.subject_id = {subject_id} and a.chapter_id = {chapter_id} and a.attempt_status="Incorrect"  and a.topic_id IN {topic_id_list} ORDER BY RAND() limit {single_total_question_cnt}'
                    #query = f'SELECT question_id,subject_id,topic_id,difficulty_level,chapter_id as category FROM question_bank_master where subject_id = {subject_id}  and chapter_id = {chapter_id} and topic_id IN {topic_id_list} ORDER BY RAND() limit {single_total_question_cnt}'
                    second_df = await conn.execute_query_dict(query)
                    second_df = pd.DataFrame(second_df)
                    second_df = second_df.reset_index(drop=True)
                    final_question = pd.concat([final_question, second_df], axis=0)
        except Exception as e:
            print(e)
            traceback.print_tb(e.__traceback__)
            pass
        final_question = final_question.head(single_total_question_cnt).reset_index(drop=True)
        # if amount of questions are not equal to asked API question we are getting random questions available from DB
        if final_question.shape[0] < single_total_question_cnt:
            differnce_count = single_total_question_cnt - final_question.shape[0]
            if (quiz_bank == "question_bank_jee") | (quiz_bank == "question_bank_neet"):

                query = f'SELECT question_id,subject_id,topic_id,difficulty_level,skill_id as category FROM {quiz_bank} where subject_id = {subject_id}  AND topic_id >0   ORDER BY RAND() '
            else:

                query = f'SELECT question_id,subject_id,topic_id,difficulty_level,skill_id as category FROM {quiz_bank} where subject_id = {subject_id}   ORDER BY RAND()'

            output_frame = await conn.execute_query_dict(query)
            new_df_second=pd.DataFrame(output_frame)
            final_question = pd.concat([final_question, new_df_second], axis=0).reset_index(drop=True)
        #final_question.drop_duplicates(subset=["question_id"], keep=False, inplace=True)
        questions = pd.DataFrame()
        if final_question.shape[0] != 0:
            if final_question.shape[0] != single_total_question_cnt:
                last_subs = final_question['subject_id'].value_counts().index[-1]
                for subs in final_question['subject_id'].value_counts().index:
                    count = single_total_question_cnt / (len(final_question['subject_id'].value_counts().index))
                    if count.is_integer():
                        single_sub = final_question[final_question['subject_id'] == subs].head(int(count)).reset_index(
                            drop=True)
                        questions = pd.concat([questions, single_sub], axis=0)
                    else:
                        last_count = single_total_question_cnt - int(count) * len(
                            final_question['subject_id'].value_counts().index)
                        if subs != last_subs:
                            single_sub = final_question[final_question['subject_id'] == subs].head(int(count)).reset_index(
                                drop=True)
                        else:
                            single_sub = final_question[final_question['subject_id'] == subs].head(
                                int(count + last_count)).reset_index(drop=True)
                        questions = pd.concat([questions, single_sub], axis=0)
            else:
                questions = final_question.copy()
        else:
            questions = []
        return list(questions['question_id'])
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=200,content={'msg': f"{e}","success":False})


@router.post('/advance-question-selection2',description="Advance Question Selection. If subjectid and chapterid are 0 fetch topic wise questions",status_code=201)
async def advance_question_selection_test2(aqst:AdvanceQuestionSelectiontest2):
    # JWT Authenticating
    try:
        conn = Tortoise.get_connection("default")
        tokenFlag = await jwtAuth(conn)
        if tokenFlag == False:
            return {"status": "Token is Expired or invalid !!"}

        start_time = datetime.now()
        getJson = jsonable_encoder(aqst)
        df_j = pd.DataFrame([getJson])
        df = df_j.copy()
        student_id_input = int(df['student_id'].iloc[0])
        exam_id_input = int(df['exam_id'].iloc[0])
        count = int(df['question_cnt'].iloc[0])
        quiz_bank=""
        exam_time_per_ques=1
        #Initializing Redis
        r = redis.Redis()
        if r.exists(str(student_id_input)+"_sid"):
            student_cache= json.loads(r.get(str(student_id_input)+"_sid"))
            #print("Redis student data: "+str(student_cache))
            if "quiz_bank" in student_cache:
                quiz_bank = student_cache['quiz_bank']
                #print(quiz_bank)
            else:
                query = f'Select question_bank_name,time_allowed,questions_cnt,exam_paper_single_mult_flag from class_exams where id ={exam_id_input}'
                df_quiz1 = await conn.execute_query_dict(query)
                df_quiz = pd.DataFrame(df_quiz1)
                quiz_bank = df_quiz['question_bank_name'].iloc[0]
                student_cache['quiz_bank']=quiz_bank
                r.setex(str(student_id_input) + "_sid",timedelta(days=1), json.dumps(student_cache))
        else:
            query = f'Select question_bank_name,time_allowed,questions_cnt,exam_paper_single_mult_flag from class_exams where id ={exam_id_input}'
            df_quiz1 = await conn.execute_query_dict(query)
            df_quiz = pd.DataFrame(df_quiz1)
            quiz_bank = df_quiz['question_bank_name'].iloc[0]
            student_cache={"exam_id":exam_id_input,"quiz_bank":quiz_bank}
            r.setex(str(student_id_input)+"_sid", timedelta(days=1),json.dumps(student_cache))
            #print("Student Data stored in redis")


        if r.exists(str(exam_id_input) + "_examid"):
            exam_cache = json.loads(r.get(str(exam_id_input) + "_examid"))
            #print("Redis exam data: "+str(exam_cache))
            if "exam_time_per_ques" in exam_cache:
                exam_time_per_ques = exam_cache['exam_time_per_ques']
            else:
                query = f'SELECT exam_time_per_ques from class_exams where id={exam_id_input}'
                df_time1 = await conn.execute_query_dict(query)
                df_time = pd.DataFrame(df_time1)
                exam_time_per_ques = df_time.iloc[0]['exam_time_per_ques']
                #print(f'exam time per question: {exam_time_per_ques}')
                exam_cache['exam_time_per_ques']=exam_time_per_ques
                r.setex(str(exam_id_input) + "_examid",timedelta(days=1), json.dumps(exam_cache))
        else:
            query = f'SELECT exam_time_per_ques from class_exams where id={exam_id_input}'
            df_time1 = await conn.execute_query_dict(query)
            class_exam_id = df_time1[0]['exam_time_per_ques']
            exam_cache={"exam_time_per_ques": exam_time_per_ques }
            #print(exam_time_per_ques)
            r.setex(str(exam_id_input) + "_examid",timedelta(days=1), json.dumps(exam_cache))
            #print("Data stored in redis: ")

        total_time = exam_time_per_ques * count


        ###If subjectid and chapterid are 0 fetch topic wise questions
        final_question = pd.DataFrame()
        if getJson['subject_id']==0 and getJson['chapter_id']==0:
            single_exam_id = df['exam_id'].iloc[0]
            single_total_question_cnt = int(df['question_cnt'].iloc[0])
            topic_id_list_int = json.loads(getJson['topic_list'])
            topic_id_list = [int(i) for i in topic_id_list_int]
            if len(topic_id_list)==1:
                topic_id_list="("+str(topic_id_list[0])+")"
            else:
                topic_id_list = tuple(topic_id_list)
            query1 = f'SELECT b.question_id FROM student_questions_attempted a INNER JOIN {quiz_bank} b ON a.question_id = b.question_id WHERE a.student_id ={student_id_input} and a.class_exam_id = {single_exam_id} and a.attempt_status="Unanswered"  and a.topic_id IN {topic_id_list} ORDER BY RAND() limit {single_total_question_cnt}'
            output_frame = await conn.execute_query_dict(query1)
            #print("Output from 1st query")
            #print(output_frame)
            new_df_second = pd.DataFrame(output_frame)
            final_question = pd.concat([final_question, new_df_second], axis=0).reset_index(drop=True)
            if final_question.shape[0] != single_total_question_cnt:
                query2 = f'SELECT question_id FROM {quiz_bank} where topic_id IN {topic_id_list}   ORDER BY RAND() '
                #print(query2)
                output_frame = await conn.execute_query_dict(query2)
                new_df_second = pd.DataFrame(output_frame)
                #print("Output from 2nd query")
                #print(output_frame)
                final_question = pd.concat([final_question, new_df_second], axis=0).reset_index(drop=True)
            final_question.drop_duplicates(subset=["question_id"], keep=False, inplace=True)
            flatten=list(final_question['question_id'])
        else:
            flatten = await get_custom_from_DB2(df, getJson['subject_id'], getJson['topic_list'], getJson['chapter_id'],quiz_bank)
        ###End of topic wise questions fetch
        # print(total_time)
        # based on test type apply filters to the question bank and fetch respective questions.
        # try:
        #print(flatten)
        if len(flatten) == 1:
            quest = "(" + str(flatten[0]) + ")"
        else:
            #print(flatten)
            quest = tuple(flatten)

        # print("Time took for execution for this API: %s seconds " % (time.time() - start_time))

        query = f'select qb.question_id, qb.subject_id,qb.chapter_id, qb.topic_id, qb.question, qb.template_type, qb.difficulty_level, \
        qb.marks, qb.negative_marking, qb.question_options,  qb.answers, \
        qb.time_allowed, qb.passage_inst_ind, qb.passage_inst_id, b.passage_inst, b.pass_inst_type \
        from {quiz_bank} qb LEFT JOIN question_bank_passage_inst b ON b.id = qb.passage_inst_id \
        where qb.question_id in {quest}'
        #print(query)
        datalist1 = await conn.execute_query_dict(query)
        data1=pd.DataFrame(datalist1)
        data1=data1.fillna(0)
        #data2 = data1.copy()
        # print(data2)
        # data2.fillna(value=0)


        l1 = str(total_time)

        l2 = data1.to_dict(orient='records')

        D = {"time_allowed": l1, "questions": l2 ,"success":True}
        jsonstr = json.dumps(l2, ensure_ascii=False).encode('utf8')
        print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))
        return D
        #return {"time_allowed": l1,"response":jsonstr,"success":True,}
        #return JSONResponse(status_code=200,content={"reponse":jsonstr,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={'msg': f"{e}","success":False})

@router.post('/planner-question-selection',description="Question Selection for planner",status_code=201)
async def planner_question_selection(plannerInput:PlannerQuestionSelection):
    student_id= plannerInput.student_id
    chapter_id= plannerInput.chapter_id
    exam_id= plannerInput.exam_id
    try:
        conn = Tortoise.get_connection('default')
        try:
            query = f'select question_bank_name from question_bank_tables where exam_id={exam_id}'
            summ = await conn.execute_query_dict(query)
            que_bank = summ[0]['question_bank_name']
        except:
            print("invalid exam id")
        #print(que_bank)
        query = f'select qb.question_id, qb.subject_id,qb.chapter_id, qb.topic_id, qb.question, qb.template_type, qb.difficulty_level, \
        qb.marks, qb.negative_marking, qb.question_options,  qb.answers, \
        qb.time_allowed, qb.passage_inst_ind, qb.passage_inst_id, b.passage_inst, b.pass_inst_type \
        from {que_bank} qb LEFT JOIN question_bank_passage_inst b ON b.id = qb.passage_inst_id \
        where chapter_id={chapter_id}  ORDER BY RAND() '
        """
        query1 = f'select a.question_id, a.class_id,  a.subject_id,a.chapter_id , a.topic_id, ' \
                 f'a.question, a.template_type, a.difficulty_level, a.language_id, a.marks, a.negative_marking, a.question_options, a.answers,' \
                 f'a.time_allowed,   a.passage_inst_ind, a.passage_inst_id, b.passage_inst, b.pass_inst_type ' \
                 f'FROM {que_bank} a left join question_bank_passage_inst b ON a.passage_inst_id = b.id ' \
                 f' where a.question_id in ' \
                 f'(select question_id from student_profiling_questions where exam_id = {exam_id}) limit {count}'
        """
        summary1 = await conn.execute_query_dict(query)
        summary1=pd.DataFrame(summary1)
        summary1=summary1.fillna(0)
        count=len(summary1.index)
        summary1=summary1.to_dict('records')
        #restructure dict on subject_id
        grouped = {}
        for dict in summary1:
            grouped.setdefault(dict['subject_id'], []).append(
                {k: v for k, v in dict.items() if k != 'subject_id'})
        grouped = [{'subject_id': k, 'Questions': v} for k, v in grouped.items()]

        #Subject List by exam ID
        query=f'select subjects.id,subjects.subject_name from exam_subject_chapters as esc join subjects on  esc.subject_id=subjects.id where chapter_id={chapter_id}'
        subject_list = await conn.execute_query_dict(query)

        query = f'SELECT exam_time_per_ques from class_exams where id={exam_id}'
        df_time1 = await conn.execute_query_dict(query)
        df_time=pd.DataFrame(df_time1)
        total_time = df_time.iloc[0] * count
        return JSONResponse(status_code=200,
                            content={"count":count, "time_allowed": int(total_time),"Subjects":subject_list, "questions_list": grouped,
                                     "success": True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})

async def get_subject_questions(subject_id, quiz_bank,student_id,question_cnt):
    try:
        conn = Tortoise.get_connection("default")
        #print("Getting subject questions")
        final_question_list=[]

        #criteria 1:  fresh questions are given to student which he has not seen before

        questions_query=f'SELECT question_id,attempt_status FROM student_questions_attempted WHERE student_id={student_id} and subject_id={subject_id}'
        attempted_questions_list=await conn.execute_query_dict(questions_query)
        attempted_questions_list=pd.DataFrame(attempted_questions_list)

        if attempted_questions_list.empty:
            #print("No attempted questions found ")
            attempted_question_ids = []
            unseen_questions_query=f'SELECT question_id FROM {quiz_bank} where subject_id={subject_id} order by rand() limit {question_cnt}'

        else:
            attempted_question_ids = attempted_questions_list['question_id'].unique().tolist()
            if len(attempted_question_ids) == 1:
                attempted_question_ids = "(" + str(attempted_question_ids[0]) + ")"
                unseen_questions_query = f'SELECT question_id FROM {quiz_bank} where question_id not in {attempted_question_ids} and subject_id={subject_id} order by rand() limit {question_cnt}'

            else:
                attempted_question_ids = tuple(attempted_question_ids)
                unseen_questions_query = f'SELECT question_id FROM {quiz_bank} where question_id not in {attempted_question_ids} and subject_id={subject_id} order by rand() limit {question_cnt}'

        question_list1= await conn.execute_query_dict(unseen_questions_query)
        question_list1=[d['question_id'] for d in question_list1 if 'question_id' in d]
        #print(question_list1)
        final_question_list.extend(question_list1)
        #print(len(final_question_list))
        if (len(final_question_list)==question_cnt):
            return final_question_list

        if final_question_list:
            remaining_questions= question_cnt - len(final_question_list)
        if  not attempted_questions_list.empty:

            #criteria 2:  If student has attempted all questions atleast once then get questions skipped by student
            await update_db_exhaust_date(student_id)
            skipped_questions_list=attempted_questions_list.loc[attempted_questions_list['attempt_status'] == "Unanswered"]
            skipped_question_ids=skipped_questions_list['question_id'].unique().tolist()
            final_question_list.extend(skipped_question_ids)
            final_question_list = set(final_question_list)
            final_question_list=list(final_question_list)
            #print(skipped_question_ids)
            if len(final_question_list)>question_cnt:
                final_question_list=final_question_list[0:question_cnt]
            if (len(final_question_list) == question_cnt):
                return final_question_list
            #print(len(final_question_list))

            remaining_questions= question_cnt - len(final_question_list)

            #criteria 3: If questions are less then get incorrect questions
            incorrect_questions_list=attempted_questions_list.loc[attempted_questions_list['attempt_status'] == "Incorrect"]
            incorrect_question_ids=incorrect_questions_list['question_id'].unique().tolist()
            final_question_list.extend(incorrect_question_ids)
            final_question_list = set(final_question_list)
            final_question_list=list(final_question_list)
            #print(skipped_question_ids)
            if len(final_question_list)>question_cnt:
                final_question_list=final_question_list[0:question_cnt]
            if (len(final_question_list) == question_cnt):
                return final_question_list
            #print(len(final_question_list))

            remaining_questions= question_cnt - len(final_question_list)
        #criteria 4: If questions are less then get random questions
        if not final_question_list:
            final_question_list=[]
        else:
            if len(final_question_list) == 1:
                final_question_list_str = "(" + str(final_question_list[0]) + ")"
            else:
                final_question_list_str = tuple(final_question_list)
                random_questions_query = f'SELECT question_id FROM {quiz_bank} where question_id not in {final_question_list_str} and subject_id={subject_id} order by rand() limit {remaining_questions}'
                question_list1 = await conn.execute_query_dict(random_questions_query)
                question_list1 = [d['question_id'] for d in question_list1 if 'question_id' in d]
                # print(question_list1)
                final_question_list.extend(question_list1)
                #print(len(final_question_list))
                return final_question_list

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})

async def get_chapter_questions(chapter_id, quiz_bank,student_id,question_cnt):
    try:
        conn = Tortoise.get_connection("default")
        #print("Getting chapter questions")
        final_question_list=[]

        #criteria 1:  fresh questions are given to student which he has not seen before

        questions_query=f'SELECT question_id,attempt_status FROM student_questions_attempted WHERE student_id={student_id} and chapter_id={chapter_id}'
        attempted_questions_list=await conn.execute_query_dict(questions_query)
        attempted_questions_list=pd.DataFrame(attempted_questions_list)

        if  attempted_questions_list.empty:
            #print("No attempted questions found ")
            attempted_question_ids=[]
            unseen_questions_query = f'SELECT question_id FROM {quiz_bank} where  chapter_id={chapter_id} order by rand() limit {question_cnt}'

        else:
            attempted_question_ids = attempted_questions_list['question_id'].unique().tolist()
            if len(attempted_question_ids) == 1:
                attempted_question_ids = "(" + str(attempted_question_ids[0]) + ")"
                unseen_questions_query = f'SELECT question_id FROM {quiz_bank} where question_id not in {attempted_question_ids} and chapter_id={chapter_id} order by rand() limit {question_cnt}'

            else:
                attempted_question_ids = tuple(attempted_question_ids)
                unseen_questions_query = f'SELECT question_id FROM {quiz_bank} where question_id not in {attempted_question_ids} and chapter_id={chapter_id} order by rand() limit {question_cnt}'
        question_list1= await conn.execute_query_dict(unseen_questions_query)
        question_list1=[d['question_id'] for d in question_list1 if 'question_id' in d]
        #print(question_list1)
        final_question_list.extend(question_list1)
        #print(len(final_question_list))
        if (len(final_question_list)==question_cnt):
            return final_question_list

        if final_question_list:
            remaining_questions= question_cnt - len(final_question_list)
        if  not attempted_questions_list.empty:
            #criteria 2:  If student has attempted all questions atleast once then get questions skipped by student
            await update_db_exhaust_date(student_id)
            skipped_questions_list=attempted_questions_list.loc[attempted_questions_list['attempt_status'] == "Unanswered"]
            skipped_question_ids=skipped_questions_list['question_id'].unique().tolist()
            final_question_list.extend(skipped_question_ids)
            final_question_list = set(final_question_list)
            final_question_list=list(final_question_list)
            #print(skipped_question_ids)
            if len(final_question_list)>question_cnt:
                final_question_list=final_question_list[0:question_cnt]
            if (len(final_question_list) == question_cnt):
                return final_question_list
            #print(len(final_question_list))

            remaining_questions= question_cnt - len(final_question_list)

            #criteria 3: If questions are less then get incorrect questions
            incorrect_questions_list=attempted_questions_list.loc[attempted_questions_list['attempt_status'] == "Incorrect"]
            incorrect_question_ids=incorrect_questions_list['question_id'].unique().tolist()
            final_question_list.extend(incorrect_question_ids)
            final_question_list = set(final_question_list)
            final_question_list=list(final_question_list)
            #print(skipped_question_ids)
            if len(final_question_list)>question_cnt:
                final_question_list=final_question_list[0:question_cnt]
            if (len(final_question_list) == question_cnt):
                return final_question_list
            #print(len(final_question_list))

        remaining_questions= question_cnt - len(final_question_list)
        #criteria 4: If questions are less then get random questions
        if not final_question_list:
            final_question_list=[]
        else:
            if len(final_question_list) == 1:
                final_question_list_str = "(" + str(final_question_list[0]) + ")"
            else:
                final_question_list_str = tuple(final_question_list)
                random_questions_query = f'SELECT question_id FROM {quiz_bank} where question_id not in {final_question_list_str} and chapter_id={chapter_id} order by rand() limit {remaining_questions}'
                question_list1 = await conn.execute_query_dict(random_questions_query)
                question_list1 = [d['question_id'] for d in question_list1 if 'question_id' in d]
                # print(question_list1)
                final_question_list.extend(question_list1)
                #print(len(final_question_list))
                return final_question_list

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})

async def get_topicid_questions(topic_id_list, quiz_bank,student_id,question_cnt):
    try:
        conn = Tortoise.get_connection("default")
        #print("Getting topic questions")
        final_question_list=[]

        #criteria 1:  fresh questions are given to student which he has not seen before

        questions_query=f'SELECT question_id,attempt_status FROM student_questions_attempted WHERE student_id={student_id} and topic_id in {topic_id_list}'
        attempted_questions_list=await conn.execute_query_dict(questions_query)
        attempted_questions_list=pd.DataFrame(attempted_questions_list)

        if attempted_questions_list.empty:
            #print("No attempted questions found ")
            attempted_question_ids=[]
            unseen_questions_query = f'SELECT question_id FROM {quiz_bank} where  topic_id in {topic_id_list} order by rand() limit {question_cnt}'

        else:
            #print("Attempted questions found ")
            attempted_question_ids = attempted_questions_list['question_id'].unique().tolist()
            if len(attempted_question_ids) == 1:
                attempted_question_ids = "(" + str(attempted_question_ids[0]) + ")"
                unseen_questions_query = f'SELECT question_id FROM {quiz_bank} where question_id not in {attempted_question_ids} and topic_id in {topic_id_list} order by rand() limit {question_cnt}'
            else:
                attempted_question_ids = tuple(attempted_question_ids)
                unseen_questions_query = f'SELECT question_id FROM {quiz_bank} where question_id not in {attempted_question_ids} and topic_id in {topic_id_list} order by rand() limit {question_cnt}'
            question_list1= await conn.execute_query_dict(unseen_questions_query)
            question_list1=[d['question_id'] for d in question_list1 if 'question_id' in d]
            #print(question_list1)
            final_question_list.extend(question_list1)
            #print(len(final_question_list))
            if (len(final_question_list)==question_cnt):
                return final_question_list

        if final_question_list:
            remaining_questions= question_cnt - len(final_question_list)
        if  not attempted_questions_list.empty:
            #criteria 2:  If student has attempted all questions atleast once then get questions skipped by student
            await update_db_exhaust_date(student_id)
            skipped_questions_list=attempted_questions_list.loc[attempted_questions_list['attempt_status'] == "Unanswered"]
            skipped_question_ids=skipped_questions_list['question_id'].unique().tolist()
            final_question_list.extend(skipped_question_ids)
            final_question_list = set(final_question_list)
            final_question_list=list(final_question_list)
            #print(skipped_question_ids)
            if len(final_question_list)>question_cnt:
                final_question_list=final_question_list[0:question_cnt]
            if (len(final_question_list) == question_cnt):
                return final_question_list
            #print(len(final_question_list))

            remaining_questions= question_cnt - len(final_question_list)

            #criteria 3: If questions are less then get incorrect questions
            incorrect_questions_list=attempted_questions_list.loc[attempted_questions_list['attempt_status'] == "Incorrect"]
            incorrect_question_ids=incorrect_questions_list['question_id'].unique().tolist()
            final_question_list.extend(incorrect_question_ids)
            final_question_list = set(final_question_list)
            final_question_list=list(final_question_list)
            #print(skipped_question_ids)
            if len(final_question_list)>question_cnt:
                final_question_list=final_question_list[0:question_cnt]
            if (len(final_question_list) == question_cnt):
                return final_question_list
            #print(len(final_question_list))

        remaining_questions= question_cnt - len(final_question_list)
        #criteria 4: If questions are less then get random questions
        if not final_question_list:
            random_questions_query = f'SELECT question_id FROM {quiz_bank} where  topic_id in {topic_id_list} order by rand() limit {remaining_questions}'
            print(random_questions_query)
            question_list1 = await conn.execute_query_dict(random_questions_query)
            question_list1 = [d['question_id'] for d in question_list1 if 'question_id' in d]
            # print(question_list1)
            final_question_list.extend(question_list1)
            # print(len(final_question_list))
            return final_question_list
        else:
            if len(final_question_list) == 1:
                final_question_list_str = "(" + str(final_question_list[0]) + ")"
            else:
                final_question_list_str = tuple(final_question_list)
                random_questions_query = f'SELECT question_id FROM {quiz_bank} where question_id not in {final_question_list_str} and topic_id in {topic_id_list} order by rand() limit {remaining_questions}'
                print(random_questions_query)
                question_list1 = await conn.execute_query_dict(random_questions_query)
                question_list1 = [d['question_id'] for d in question_list1 if 'question_id' in d]
                # print(question_list1)
                final_question_list.extend(question_list1)
                #print(len(final_question_list))
                return final_question_list

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})


@router.post('/custom-question-selection',description="Advance Question Selection. If subjectid and chapterid are 0 fetch topic wise questions",status_code=201)
async def custom_question_selection_test(aqst:AdvanceQuestionSelectiontest2):
    # JWT Authenticating
    try:
        conn = Tortoise.get_connection("default")
        start_time = datetime.now()
        getJson = jsonable_encoder(aqst)
        df_j = pd.DataFrame([getJson])
        df = df_j.copy()
        student_id_input = int(df['student_id'].iloc[0])
        exam_id_input = int(df['exam_id'].iloc[0])
        count = int(df['question_cnt'].iloc[0])
        subject_id=getJson['subject_id']
        chapter_id=getJson['chapter_id']
        quiz_bank=""
        exam_time_per_ques=1
        #Initializing Redis
        r = redis.Redis()
        if r.exists(str(student_id_input)+"_sid"):
            student_cache= json.loads(r.get(str(student_id_input)+"_sid"))
            #print("Redis student data: "+str(student_cache))
            if "quiz_bank" in student_cache:
                quiz_bank = student_cache['quiz_bank']
                #print(quiz_bank)
            else:
                query = f'Select question_bank_name from class_exams where id ={exam_id_input}'
                df_quiz1 = await conn.execute_query_dict(query)
                df_quiz = pd.DataFrame(df_quiz1)
                quiz_bank = df_quiz['question_bank_name'].iloc[0]
                student_cache={"quiz_bank":quiz_bank}
                r.setex(str(student_id_input) + "_sid", timedelta(days=1), json.dumps(student_cache))
        else:
            query = f'Select question_bank_name from class_exams where id ={exam_id_input}'
            df_quiz1 = await conn.execute_query_dict(query)
            df_quiz = pd.DataFrame(df_quiz1)
            quiz_bank = df_quiz['question_bank_name'].iloc[0]
            student_cache={"exam_id":exam_id_input,"quiz_bank":quiz_bank}
            r.setex(str(student_id_input)+"_sid", timedelta(days=1), json.dumps(student_cache))
            #print("Student Data stored in redis")


        if r.exists(str(exam_id_input) + "_examid"):
            exam_cache = json.loads(r.get(str(exam_id_input) + "_examid"))
            #print("Redis exam data: "+str(exam_cache))
            if "exam_time_per_ques" in exam_cache:
                exam_time_per_ques = exam_cache['exam_time_per_ques']
            else:
                query = f'SELECT exam_time_per_ques from class_exams where id={exam_id_input}'
                df_time1 = await conn.execute_query_dict(query)
                exam_time_per_ques = df_time1[0]['exam_time_per_ques']
                exam_cache={"exam_time_per_ques":exam_time_per_ques}
                #print(exam_cache)
                r.setex(str(exam_id_input) + "_examid", timedelta(days=1), json.dumps(exam_cache))
        else:
            query = f'SELECT exam_time_per_ques,time_allowed,questions_cnt,question_bank_name from class_exams where id={exam_id_input}'
            df_time1 = await conn.execute_query_dict(query)
            exam_time_per_ques = df_time1[0]['exam_time_per_ques']
            time_allowed = df_time1[0]['time_allowed']
            questions_cnt = df_time1[0]['questions_cnt']
            question_bank_name = df_time1[0]['question_bank_name']
            exam_cache={"exam_time_per_ques": exam_time_per_ques,"time_allowed":time_allowed,
                        "questions_cnt":questions_cnt,"question_bank_name":question_bank_name }
            #print(df_time1)
            r.setex(str(exam_id_input) + "_examid", timedelta(days=1), json.dumps(exam_cache))
            #print("Data stored in redis: ")

        total_time = exam_time_per_ques * count
        ###If subjectid and chapterid are 0 fetch topic wise questions
        final_question = pd.DataFrame()
        topic_id_list_int = getJson['topic_list']
        if not topic_id_list_int:
            #print("Empty topic list")
            topic_id_list=[]
        else:
            topic_id_list_int=json.loads(topic_id_list_int)
            topic_id_list = [int(i) for i in topic_id_list_int]
            if len(topic_id_list) == 1:
                topic_id_list = "(" + str(topic_id_list[0]) + ")"
            else:
                topic_id_list = tuple(topic_id_list)
        if subject_id== 0 and chapter_id == 0:
            #print("Getting questions as per topic list")
            #print(topic_id_list)
            questions_list=await get_topicid_questions(topic_id_list, quiz_bank, student_id_input, count)

        if subject_id == 0 and len(topic_id_list)==0:
            #print("Getting questions as per chapter_id")
            questions_list=await get_chapter_questions(chapter_id, quiz_bank, student_id_input, count)

        if chapter_id == 0 and len(topic_id_list)==0:
            #print("Getting questions as per subject_id")
            questions_list=await get_subject_questions(subject_id, quiz_bank, student_id_input, count)
        print(questions_list)
        if questions_list:
            total_time = exam_time_per_ques * len(questions_list)

            if len(questions_list) == 1:
                question_list_str = "(" + str(questions_list[0]) + ")"
            else:
                #print(questions_list)
                question_list_str = tuple(questions_list)

                query = f'select qb.question_id, qb.subject_id,qb.chapter_id, qb.topic_id, qb.question, qb.template_type, qb.difficulty_level, \
                qb.marks, qb.negative_marking, qb.question_options,  qb.answers, \
                qb.time_allowed, qb.passage_inst_ind, qb.passage_inst_id, b.passage_inst, b.pass_inst_type \
                from {quiz_bank} qb LEFT JOIN question_bank_passage_inst b ON b.id = qb.passage_inst_id \
                where qb.question_id in {question_list_str}'
                # print(query)
                datalist1 = await conn.execute_query_dict(query)
                data1 = pd.DataFrame(datalist1)
                data1 = data1.fillna(0)
                l1 = str(total_time)
                l2 = data1.to_dict(orient='records')

                response = {"time_allowed": l1, "questions": l2, "success": True}
                jsonstr = json.dumps(l2, ensure_ascii=False).encode('utf8')
                print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))
                return response
        else:
            resp = {
                "message": "No questions found for this criteria",
                "success": False
            }
            return resp
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})