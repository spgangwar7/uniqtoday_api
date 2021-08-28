import json
import traceback
from http import HTTPStatus
from typing import List

import numpy as np
import pandas as pd
import redis
from fastapi import APIRouter,BackgroundTasks
from IPython.display import display, HTML
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import IntegrityError
from fastapi.responses import JSONResponse
from tortoise import Tortoise, fields, run_async
from datetime import datetime,timedelta
from schemas.SaveResult import SaveResult

router = APIRouter(
    prefix='/api',
    tags=['Save Result'],
)
def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True

@router.post('/save-result', description='Save result', status_code=201)
async def save_result(data:SaveResult,background_tasks: BackgroundTasks):
    try:
        start_time=datetime.now()
        conn = Tortoise.get_connection("default")
        user_id = data.user_id
        test_time = data.test_time
        time_taken = data.time_taken
        class_id = data.class_id
        test_type=data.test_type
        exam_mode=data.exam_mode
        exam_type=data.exam_type
        all_questions_list =data.questions_list
        no_of_question = data.no_of_question
        answerList = data.answerList
        exam_cache={}
        #Initializing Redis
        r = redis.Redis()
        if r.exists(str(class_id) + "_examid"):
            exam_cache = json.loads(r.get(str(class_id) + "_examid"))
            if "question_bank_name" in exam_cache:
                classTablename = exam_cache['question_bank_name']
            else:
                query_class_exam_data = f"SELECT question_bank_name FROM class_exams WHERE id = {class_id}"
                class_exam_data = await conn.execute_query_dict(query_class_exam_data)
                classTablename = class_exam_data[0].get("question_bank_name")
                exam_cache['question_bank_name']=classTablename
                r.set(str(class_id) + "_examid", json.dumps(exam_cache))
        else:
            query_class_exam_data = f"SELECT question_bank_name FROM class_exams WHERE id = {class_id}"
            class_exam_data = await conn.execute_query_dict(query_class_exam_data)
            classTablename = class_exam_data[0].get("question_bank_name")
            exam_cache['question_bank_name'] = classTablename
            r.set(str(class_id) + "_examid", json.dumps(exam_cache))

        Query = f"SELECT question_id, subject_id,topic_id,chapter_id, marks,negative_marking," \
                f" template_type,answers,question_options \
            FROM {classTablename} WHERE question_id IN {tuple(all_questions_list)}"

        Question_attemt_record = await conn.execute_query_dict(Query)
        Question_attemt_recorddf = pd.DataFrame(Question_attemt_record)
        Question_attemt_recorddf=Question_attemt_recorddf.fillna(0)
        Question_attemt_record=Question_attemt_recorddf.set_index('question_id')
        answerList_copy = answerList.copy()

        total_correctAttempt = 0
        total_incorrectAttempt = 0
        marks_gain = 0
        ans_swap_count = 0
        new_answer_list=[]
        for val in answerList_copy:
            dict={}
            gain_mark = 0
            quesId = int(val.question_id)
            template_type=Question_attemt_record['template_type'].loc[quesId]
            if template_type == 3:
                correctAnswervalue = (Question_attemt_record['answers'].loc[quesId]).strip('\"')
                #print(correctAnswervalue)
                question_options=(Question_attemt_record['question_options'].loc[quesId])
                question_options=json.loads(question_options)
                for key, value in question_options.items():
                    if correctAnswervalue == value:
                        correctAnswer=value
            else:
                correctOptDict = (Question_attemt_record['answers'].loc[quesId])
                #print(correctOptDict)
                correctOptDict=json.loads(correctOptDict)
                correctAnswer = list(correctOptDict.keys())[0]

            marks = Question_attemt_record['marks'].loc[quesId]
            negative_marking = Question_attemt_record['negative_marking'].loc[quesId]
            attemptCount = int(val.attemptCount)
            ans_swap_count = ans_swap_count + attemptCount

            correct_attempt, incorrect_attempt = 0, 0
            getAnswer = str(int(val.answer))
            if correctAnswer == getAnswer:
                correct_attempt = 1
                marks_gain = int(marks_gain + marks)
                gain_mark = int(marks)
                total_correctAttempt += 1
            else:
                incorrect_attempt = 1
                marks_gain = int(marks_gain + negative_marking)
                gain_mark = int(negative_marking)
                total_incorrectAttempt += 1

            subject_id = Question_attemt_record['subject_id'].loc[quesId]
            chapter_id = Question_attemt_record['chapter_id'].loc[quesId]
            topic_id = Question_attemt_record['topic_id'].loc[quesId]
            dict={'question_id': quesId, 'subject_id': subject_id,"chapter_id":chapter_id, 'topic_id': topic_id, 'gain_mark': gain_mark,
                        "attempt_correct": correct_attempt, 'attemtpt_incorrect_cnt': incorrect_attempt, "attempt_cnt": 1,
                        "marks": marks, "negative_marking": negative_marking}
            dict.update(jsonable_encoder(val))
            new_answer_list.append(dict)
            #print(quesId, 'gain_mark=', gain_mark, ";", "attempt_cnt=1;", "attempt_correct=", correct_attempt,'incorrect_attempt=', incorrect_attempt)

        # Inserting on db user_results

        new_answer_listdf=pd.DataFrame(new_answer_list)
        pd.options.display.max_columns = None
        pd.options.display.width = None
        #print(new_answer_listdf)
        unattmepted_ques_cnt = no_of_question - (total_correctAttempt + total_incorrectAttempt)
        total_exam_marks = int(new_answer_list[0]["marks"]) * no_of_question
        result_percentage = int(round((marks_gain / int(total_exam_marks)) * 100))
        if result_percentage < 0: result_percentage = 0
        query_insert = f"INSERT INTO user_result (user_id,class_grade_id,test_type,exam_mode,no_of_question, correct_ans, incorrect_ans, unattempted_ques_cnt, marks_gain, test_time, time_taken, result_percentage, ans_swap_count ) \
                        VALUES ({user_id},{class_id},'{test_type}','{exam_mode}',{no_of_question},{total_correctAttempt},{total_incorrectAttempt},{unattmepted_ques_cnt},{marks_gain},'{test_time}','{time_taken}', {result_percentage}, {ans_swap_count} )"
        qryExecute=await conn.execute_query(query_insert)
        if not qryExecute:
            qryExecute=0
        else:
            qryExecute=1

        resultId = 0000
        # getting result_id
        student_cache={}
        if qryExecute == 1:
            query_resultId = "SELECT id FROM user_result ORDER BY id DESC LIMIT 1"
            resultId = await conn.execute_query_dict(query_resultId)
            resultId = int(resultId[0]['id'])
            if r.exists(str(user_id)+"_sid"):
                student_cache=json.loads(r.get(str(user_id)+"_sid"))
                student_cache['result_id']=resultId
                r.set(str(user_id)+"_sid",json.dumps(student_cache))
            else:
                student_cache['result_id']=resultId
                r.set(str(user_id)+"_sid",json.dumps(student_cache))

        # Inserting on Db student_questions_attempted for each of ques
        unattempted_questions_list = all_questions_list.copy()
        for quesDict in new_answer_list:
            question_id = int(quesDict['question_id'])
            answer={"Answer:":quesDict['answer']}
            answer=json.dumps(answer)
            if question_id in unattempted_questions_list: unattempted_questions_list.remove(question_id)
            subject_id = int(quesDict['subject_id'])
            topic_id = int(quesDict['topic_id'])
            attempt_cnt = int(quesDict['attempt_cnt'])
            attempt_correct = int(quesDict['attempt_correct'])
            attempt_incorrect_cnt = int(quesDict['attemtpt_incorrect_cnt'])
            question_marks = int(quesDict['marks'])
            gain_marks = int(quesDict['gain_mark'])
            time_taken_sec = str(quesDict['timetaken'])
            answer_swap_cnt = int(quesDict['attemptCount'])
            if attempt_correct==1:

                qry_update = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,student_result_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,time_taken,answer_swap_cnt,attempt_status,option_id) \
                               VALUES ({class_id},{user_id},{resultId},{subject_id},{chapter_id},{topic_id},'{exam_type}',{question_id},{question_marks}, {gain_marks}, '{time_taken_sec}',{answer_swap_cnt},'Correct','{answer}')"
                await conn.execute_query_dict(qry_update)
            else:
                qry_update = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,student_result_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,negative_marks_cnt,time_taken,answer_swap_cnt,attempt_status,option_id) \
                                               VALUES ({class_id},{user_id},{resultId},{subject_id},{chapter_id},{topic_id},'{exam_type}',{question_id},{question_marks}, {gain_marks},1,'{time_taken_sec}',{answer_swap_cnt},'Incorrect','{answer}' )"
                await conn.execute_query_dict(qry_update)

        student_result={}
        student_result["result_id"]=resultId
        student_result["no_of_question"] = int(no_of_question)
        student_result["correct_count"] = int(total_correctAttempt)
        student_result["correct_score"] = int(new_answer_list[0]["marks"] * total_correctAttempt)
        student_result["wrong_count"] = int(total_incorrectAttempt)
        student_result["incorrect_score"] = int(new_answer_list[0]["negative_marking"] * total_incorrectAttempt)
        student_result["total_exam_marks"] = int(total_exam_marks)
        student_result["total_get_marks"] = marks_gain
        student_result["result_time_taken"] = time_taken
        student_result["result_percentage"] = result_percentage
        student_result["not_answered"] = len(list(map(int, unattempted_questions_list)))
        r.setex(str(user_id) + "_sid" + "_result_data",timedelta(days=1),json.dumps(student_result))
        # inserting for unattempted quest
        message_str=""
        for unattemptQues in unattempted_questions_list:
            chapter_id = Question_attemt_record.loc[unattemptQues]['chapter_id']
            subject_id = Question_attemt_record.loc[unattemptQues]['subject_id']
            topic_id = Question_attemt_record.loc[unattemptQues]['topic_id']
            question_marks = int(new_answer_list[0]['marks'])
            qry_insert2 = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,student_result_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,negative_marks_cnt,time_taken,answer_swap_cnt,attempt_status) \
                               VALUES ({class_id},{user_id},{resultId},{subject_id},{chapter_id},{topic_id},'{exam_type}',{unattemptQues},{question_marks}, 0, 0, '00:00:00',0,'Unanswered')"
            await conn.execute_query_dict(qry_insert2)
        message_str=f'Result saved successfully. Result_ID: {resultId}'
        background_tasks.add_task(save_student_summary, user_id, class_id)
        resp = {

            "message":message_str,
            "success":True

            }
        print(f"execution time is {(datetime.now()-start_time)}")
        return resp
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error": f"{e}","success":False})

@router.post('/save-student-summary', description='Save Student summary', status_code=201)
async def save_student_summary(student_id:int,exam_id:int):
    try:
        start_time=datetime.now()
        conn = Tortoise.get_connection("default")
        #Initializing Redis
        r = redis.Redis()
        exam_cache={}
        classTablename=""
        if r.exists(str(exam_id) + "_examid"):
            exam_cache = json.loads(r.get(str(exam_id) + "_examid"))
            if "question_bank_name" in exam_cache:
                classTablename = exam_cache['question_bank_name']
            else:
                query_class_exam_data = f"SELECT question_bank_name FROM class_exams WHERE id = {exam_id}"
                class_exam_data = await conn.execute_query_dict(query_class_exam_data)
                classTablename = class_exam_data[0].get("question_bank_name")
                exam_cache['question_bank_name']=classTablename
                r.set(str(exam_id) + "_examid", json.dumps(exam_cache))
        else:
            query_class_exam_data = f"SELECT question_bank_name FROM class_exams WHERE id = {exam_id}"
            class_exam_data = await conn.execute_query_dict(query_class_exam_data)
            classTablename = class_exam_data[0].get("question_bank_name")
            exam_cache['question_bank_name'] = classTablename
            r.set(str(exam_id) + "_examid", json.dumps(exam_cache))
        result2=[]

        query = f'SELECT id,class_exam_id,student_id,student_result_id,sqa.subject_id,sqa.chapter_id,sqa.topic_id,exam_type,' \
                f'sqa.question_id,attempt_status,sqa.gain_marks,unit_id,skill_id,difficulty_level,major_concept_id,sqa.created_on FROM ' \
                f'student_questions_attempted as sqa left join {classTablename} as qbj on sqa.question_id=qbj.question_id ' \
                f'where student_id={student_id} and class_exam_id={exam_id}'
        result= await conn.execute_query_dict(query)
        resultdf=pd.DataFrame(result)
        resultdf["created_on"]=pd.to_datetime(resultdf["created_on"]).dt.strftime('%Y-%m-%d')
        if resultdf.empty:
            return JSONResponse(status_code=400,content={"response":"invalid credentials","success":False})
        print(len(resultdf))
        #print(resultdf)
        resultdf=resultdf.fillna(0)

        dfgrouponehot = pd.get_dummies(resultdf, columns=['attempt_status'], prefix=['attempt_status'])
        #dfgrouponehot = dfgrouponehot.fillna(0)
        if 'attempt_status_Correct' not in dfgrouponehot:
            dfgrouponehot['attempt_status_Correct'] = 0
        if 'attempt_status_Incorrect' not in dfgrouponehot:
            dfgrouponehot['attempt_status_Incorrect'] = 0
        if 'attempt_status_Unanswered' not in dfgrouponehot:
            dfgrouponehot['attempt_status_Unanswered'] = 0
        #print(dfgrouponehot.isnull().sum(axis = 0))
        pivotdf = dfgrouponehot.pivot_table(
            values=['attempt_status_Correct', 'attempt_status_Incorrect', 'attempt_status_Unanswered'],
            index=['student_id', 'class_exam_id', 'subject_id', 'unit_id', 'chapter_id', 'topic_id', 'skill_id',
                   'difficulty_level', 'major_concept_id', 'created_on', 'gain_marks'],
            columns=[],
            aggfunc='sum')
        await del_question(student_id)
        newdf = pd.DataFrame(pivotdf.to_records())

        newdf=newdf.to_dict('records')
        for dict in newdf:
            student_id=dict['student_id']
            class_exam_id=dict['class_exam_id']
            subject_id=dict['subject_id']
            unit_id=dict['unit_id']
            chapter_id=dict['chapter_id']
            topic_id=dict['topic_id']
            skill_id=dict['skill_id']
            difficulty_level=dict['difficulty_level']
            major_concept_id=dict['major_concept_id']
            created_on=dict['created_on']
            gain_marks=dict['gain_marks']
            attempt_status_Correct=dict['attempt_status_Correct']
            attempt_status_Incorrect=dict['attempt_status_Incorrect']
            attempt_status_Unanswered=dict['attempt_status_Unanswered']
            quesattempted=int(attempt_status_Correct)+int(attempt_status_Incorrect)+int(attempt_status_Unanswered)
            query_insert = f'INSERT INTO student_performance_summary (student_id, exam_id, subject_id, unit_id, chapter_id, \
            topic_id, skill_id, ques_difficulty_level, major_concept_id,last_test_date,question_attempted, ques_ans_correctly, \
            ques_ans_incorrectly, ques_unattempted_cnt,marks) VALUES ({student_id},{class_exam_id},{subject_id},{unit_id}, \
            {chapter_id},{topic_id},{skill_id},{difficulty_level},{major_concept_id},"{created_on}",{quesattempted},{attempt_status_Correct}, \
            {attempt_status_Incorrect},{attempt_status_Unanswered},{gain_marks})'
            await conn.execute_query_dict(query_insert)

        resp={"response":"Records inserted in db successfully"
            ,"success":True}
        print(f"execution time is {(datetime.now()-start_time)}")

        return resp
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})


async def del_question(student_id):
    try:
        conn = Tortoise.get_connection("default")
        del_query = f'DELETE FROM student_performance_summary WHERE student_id={student_id}'
        await conn.execute_query_dict(del_query)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)