import json
import traceback
from http import HTTPStatus
from typing import List

import numpy as np
import pandas as pd
import redis
from fastapi import APIRouter,BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from tortoise import Tortoise
from datetime import datetime,date,timedelta
from schemas.SaveResult import SaveResult
from queues.worker import save_summary_task
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
        print(data.dict())
        start_time=datetime.now()
        conn = Tortoise.get_connection("default")
        user_id = data.user_id
        test_time = data.test_time
        time_taken = data.time_taken
        class_id = data.class_id
        test_type=data.test_type
        exam_mode=data.exam_mode
        exam_type=data.exam_type
        planner_id=data.planner_id
        live_exam_id=data.live_exam_id
        all_questions_list =data.questions_list
        all_questions_list_str=""
        if len(all_questions_list) == 1:
            all_questions_list_str = "(" + str(all_questions_list[0]) + ")"
        else:
            all_questions_list_str = tuple(all_questions_list)
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
                r.setex(str(class_id) + "_examid",timedelta(days=1),json.dumps(exam_cache))
        else:
            query_class_exam_data = f"SELECT question_bank_name FROM class_exams WHERE id = {class_id}"
            class_exam_data = await conn.execute_query_dict(query_class_exam_data)
            classTablename = class_exam_data[0].get("question_bank_name")
            exam_cache['question_bank_name'] = classTablename
            r.setex(str(class_id) + "_examid",timedelta(days=1), json.dumps(exam_cache))

        Query = f"SELECT question_id, subject_id,topic_id,chapter_id, marks,negative_marking," \
                f" template_type,answers,question_options \
            FROM {classTablename} WHERE question_id IN {all_questions_list_str}"

        Question_attemt_record = await conn.execute_query_dict(Query)
        Question_attemt_recorddf = pd.DataFrame(Question_attemt_record)
        Question_attemt_recorddf=Question_attemt_recorddf.fillna(0)
        Question_attemt_record=Question_attemt_recorddf.set_index('question_id')
        answerList_copy = answerList.copy()
        #print(Question_attemt_record)
        total_correctAttempt = 0
        total_incorrectAttempt = 0
        marks_gain = 0
        ans_swap_count = 0
        marks=0
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
        if answerList_copy:
            marks = int(new_answer_list[0]["marks"])
            total_exam_marks=int(Question_attemt_recorddf['marks'].sum())
            if total_exam_marks==0:
                result_percentage=0
            else:
                result_percentage = int(round((marks_gain / int(total_exam_marks)) * 100))
            correct_score=int(marks * total_correctAttempt)
            incorrect_score=int(new_answer_list[0]["negative_marking"] * total_incorrectAttempt)
        else:
            marks=Question_attemt_record.iloc[0]["marks"]
            correct_score=int(marks * total_correctAttempt)
            incorrect_score=int(Question_attemt_record.iloc[0]["negative_marking"] * total_incorrectAttempt)
            total_exam_marks=int(Question_attemt_recorddf['marks'].sum())
            result_percentage=0

        if result_percentage < 0: result_percentage = 0
        query_insert=""
        if test_type=="Live":
            query_insert = f"INSERT INTO user_result (user_id,class_grade_id,test_type,exam_mode,no_of_question, correct_ans, incorrect_ans, unattempted_ques_cnt, marks_gain, test_time, time_taken, result_percentage, ans_swap_count,live_exam_id ) \
                                   VALUES ({user_id},{class_id},'{test_type}','{exam_mode}',{no_of_question},{total_correctAttempt},{total_incorrectAttempt},{unattmepted_ques_cnt},{marks_gain},'{test_time}','{time_taken}', {result_percentage}, {ans_swap_count},{live_exam_id} )"
        else:
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
                r.setex(str(user_id)+"_sid",timedelta(days=1),json.dumps(student_cache))
            else:
                student_cache['result_id']=resultId
                r.setex(str(user_id)+"_sid",timedelta(days=1),json.dumps(student_cache))

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
        student_result["correct_score"] = correct_score
        student_result["result_id"]=resultId
        student_result["no_of_question"] = int(no_of_question)
        student_result["correct_count"] = int(total_correctAttempt)
        student_result["wrong_count"] = int(total_incorrectAttempt)
        student_result["incorrect_score"] = incorrect_score
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
            question_marks = marks
            qry_insert2 = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,student_result_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,negative_marks_cnt,time_taken,answer_swap_cnt,attempt_status) \
                               VALUES ({class_id},{user_id},{resultId},{subject_id},{chapter_id},{topic_id},'{exam_type}',{unattemptQues},{question_marks}, 0, 0, '00:00:00',0,'Unanswered')"
            await conn.execute_query_dict(qry_insert2)
        today_date = datetime.strftime(date.today(), '%y-%m-%d')
        if test_type=="Profiling":
            query="update student_preferences set prof_asst_test={},prof_test_date={},prof_test_marks={} where student_id={}".format(2,"'"+today_date+"'",marks_gain,user_id)
            await conn.execute_query(query)
        elif test_type=="Scholarship":
            query="update student_preferences set scholar_test_date={},scholar_test_status={},scholarship_test_marks={} where student_id={}".format("'"+today_date+"'",1,marks_gain,user_id)
            await conn.execute_query(query)
        elif test_type=="Planner":
            query=f"update student_planner set test_completed_yn='Y' where student_id={user_id} and id={planner_id}"
            await conn.execute_query(query)

        message_str=f'Result saved successfully. Result_ID: {resultId}'
        #background_tasks.add_task(save_student_summary, user_id, class_id)
        task=save_summary_task.delay(user_id, class_id)
        print(task.id)
        resp = {

            "message":message_str,
            "result_id":resultId,
            "success":True

            }
        print(f"execution time is {(datetime.now()-start_time)}")
        return resp
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error": f"{e}","success":False})
