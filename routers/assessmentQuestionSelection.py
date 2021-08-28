import traceback
from datetime import timedelta
from http import HTTPStatus
import pandas as pd
import redis
from schemas.AssessmentQuestions import AssessmentQuestions
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from collections import defaultdict
import redis
import json
from datetime import datetime
router = APIRouter(
    prefix='/api',
    tags=['AssessmentTest'],
)

@router.post('/assessment-question-selection',description="Assessment Question Selection",status_code=201)
async def AssessmentQuestionSelection(assessmentInput:AssessmentQuestions):
    try:
        start_time = datetime.now()
        conn = Tortoise.get_connection('default')
        r = redis.Redis()
        exam_id=assessmentInput.exam_id
        student_id=assessmentInput.student_id
        time_allowed,questions_cnt,question_bank_name="","",""
        exhausted_query = f'Select question_bank_exhausted_flag from student_preferences where student_id = {student_id}'
        df_exhausted = await conn.execute_query_dict(exhausted_query)
        question_bank_exhausted_flag = df_exhausted[0]['question_bank_exhausted_flag']
        if question_bank_exhausted_flag :
            if question_bank_exhausted_flag == "Yes":
                question_bank_exhausted_flag=True
            else:
                question_bank_exhausted_flag = False
        else:
            question_bank_exhausted_flag=False
        print("question_bank_exhausted_flag: " + str(question_bank_exhausted_flag))

        if r.exists(str(exam_id) + "_examid"):
            exam_cache = json.loads(r.get(str(exam_id) + "_examid"))
            # print("Redis exam data: "+str(exam_cache))
            if "time_allowed" in exam_cache and "questions_cnt" in exam_cache and "question_bank_name" in exam_cache:
                time_allowed = exam_cache['time_allowed']
                questions_cnt = exam_cache['questions_cnt']
                question_bank_name = exam_cache['question_bank_name']
            else:
                query = f'SELECT exam_time_per_ques,time_allowed,questions_cnt,question_bank_name from class_exams where id={exam_id}'
                df_time1 = await conn.execute_query_dict(query)
                exam_time_per_ques = df_time1[0]['exam_time_per_ques']
                time_allowed = df_time1[0]['time_allowed']
                questions_cnt = df_time1[0]['questions_cnt']
                question_bank_name = df_time1[0]['question_bank_name']
                exam_cache={"exam_time_per_ques": exam_time_per_ques,"time_allowed":time_allowed,
                            "questions_cnt":questions_cnt,"question_bank_name":question_bank_name }
                print(df_time1)
                r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))
        else:
            query = f'SELECT exam_time_per_ques,time_allowed,questions_cnt,question_bank_name from class_exams where id={exam_id}'
            df_time1 = await conn.execute_query_dict(query)
            exam_time_per_ques = df_time1[0]['exam_time_per_ques']
            time_allowed = df_time1[0]['time_allowed']
            questions_cnt = df_time1[0]['questions_cnt']

            question_bank_name = df_time1[0]['question_bank_name']
            exam_cache = {"exam_time_per_ques": exam_time_per_ques, "time_allowed": time_allowed,
                          "questions_cnt": questions_cnt, "question_bank_name": question_bank_name}

            r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))
            # print("Data stored in redis: ")
        #check if student has given any test before

        check_test_query=f'SELECT id FROM user_result where user_id={student_id}'
        test_status = await conn.execute_query_dict(check_test_query)

        if len(test_status) == 0:
            #########################################################################################################
            #case 1
            #If student is new
            print("case 1")
            query1 = f'select qb.question_id, qb.subject_id,qb.chapter_id, qb.topic_id, qb.question, qb.template_type, qb.difficulty_level, \
                            qb.marks, qb.negative_marking, qb.question_options,  qb.answers, \
                            qb.time_allowed, qb.passage_inst_ind, qb.passage_inst_id, b.passage_inst, b.pass_inst_type \
                            from {question_bank_name} qb LEFT JOIN question_bank_passage_inst b ON b.id = qb.passage_inst_id \
                            order by rand() limit {questions_cnt}'

            result1 = await conn.execute_query_dict(query1)
            result1=pd.DataFrame(result1)
            if result1.empty:
                return JSONResponse(status_code=400,
                                    content={"response": "insufficent data or something wrong", "success": False})
            subject_id_list = result1['subject_id'].unique()
            if len(subject_id_list) > 1:
                subject_id_list = tuple(subject_id_list)
            elif len(subject_id_list) == 1:
                subject_id_list = subject_id_list[0]
                subject_id_list = "(" + str(subject_id_list) + ")"
            result1=result1.fillna(0)
            result1 = result1.to_dict('records')

            # Subject List by exam ID
            query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} and subject_id in {subject_id_list} group by exam_subjects.subject_id'
            subject_list = await conn.execute_query_dict(query)
            response = {"time_allowed": int(time_allowed), "Subjects": subject_list, "questions_list": result1,
                        "success": True}
            print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))
            return JSONResponse(status_code=200,
                                content=response)

        if question_bank_exhausted_flag :
            #########################################################################################################
            #User has exhausted question bank
            print("Case 3")

            getTopicsQuery=f'SELECT m.topic_id  FROM student_performance_summary m LEFT JOIN student_performance_summary b \
                           ON m.topic_id = b.topic_id  AND m.last_test_date < b.last_test_date \
                           WHERE b.last_test_date IS NULL and m.student_id={student_id} and m.ques_ans_incorrectly !=0 order by topic_id'
            topicslist = await conn.execute_query_dict(getTopicsQuery)
            topic_id_list = [d['topic_id'] for d in topicslist if 'topic_id' in d]

            if topic_id_list:
                if len(topic_id_list) == 1:
                    topic_id_list = "(" + str(topic_id_list[0]) + ")"
                else:
                    topic_id_list = tuple(topic_id_list)
            else:
                print("Please check if the question_bank_exhausted_flag is set correctly for user")
                return JSONResponse(status_code=400,
                                    content={"response": "Please check if the question_bank_exhausted_flag is set correctly for user", "success": False})
            query2 = f'select qb.question_id, qb.subject_id,qb.chapter_id, qb.topic_id, qb.question, qb.template_type, qb.difficulty_level, \
                                 qb.marks, qb.negative_marking, qb.question_options,  qb.answers, \
                                 qb.time_allowed, qb.passage_inst_ind, qb.passage_inst_id, b.passage_inst, b.pass_inst_type \
                                 from {question_bank_name} qb LEFT JOIN question_bank_passage_inst b ON b.id = qb.passage_inst_id \
                                 where qb.topic_id in {topic_id_list} order by rand() limit {questions_cnt}'

            result2 = await conn.execute_query_dict(query2)
            result2=pd.DataFrame(result2)
            if result2.empty:
                return JSONResponse(status_code=400,
                                    content={"response": "insufficent data or something wrong", "success": False})
            subject_id_list = result2['subject_id'].unique()
            if len(subject_id_list) > 1:
                subject_id_list = tuple(subject_id_list)
            elif len(subject_id_list) == 1:
                subject_id_list = subject_id_list[0]
                subject_id_list = "(" + str(subject_id_list) + ")"
            result2=result2.fillna(0)
            result2 = result2.to_dict('records')

            # Subject List by exam ID
            query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} and subject_id in {subject_id_list} group by exam_subjects.subject_id'
            subject_list = await conn.execute_query_dict(query)
            response = {"time_allowed": int(time_allowed), "Subjects": subject_list, "questions_list": result2,
                        "success": True}
            print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))
            return JSONResponse(status_code=200,
                                content=response)
        else:
            #########################################################################################################
            #User has attempted some questions and based on his performance fetch questions
            print("Case 2")
            query = f"SELECT topic_id FROM " \
                    f"(SELECT student_id,subject_id,topic_id,qbank_ques_count,question_attempted,ques_ans_correctly," \
                    f"(ques_ans_correctly/question_attempted)*100 AS correct_pct,ques_ans_incorrectly," \
                    f"(ques_ans_incorrectly/question_attempted)*100 AS incorrect_pct FROM student_performance_summary " \
                    f"where student_id={student_id} GROUP BY topic_id)" \
                    f" a WHERE a.incorrect_pct>50 or a.correct_pct<50"
            topicslist = await conn.execute_query_dict(query)
            topic_id_list = [d['topic_id'] for d in topicslist if 'topic_id' in d]
            print(topic_id_list)
            if topic_id_list:
                if len(topic_id_list) == 1:
                    topic_id_list = "(" + str(topic_id_list[0]) + ")"
                else:
                    topic_id_list = tuple(topic_id_list)
            else:
                print("Please check if the question_bank_exhausted_flag is set correctly for user")
                return JSONResponse(status_code=400,
                                    content={
                                        "response": "Please check if the question_bank_exhausted_flag is set correctly for user",
                                        "success": False})

            query3 = f'select qb.question_id, qb.subject_id,qb.chapter_id, qb.topic_id, qb.question, qb.template_type, qb.difficulty_level,' \
                     f'qb.marks, qb.negative_marking, qb.question_options,  qb.answers,' \
                     f'qb.time_allowed, qb.passage_inst_ind, qb.passage_inst_id, b.passage_inst, b.pass_inst_type ' \
                     f'from {question_bank_name} qb LEFT JOIN question_bank_passage_inst b ON b.id = qb.passage_inst_id ' \
                     f'where qb.topic_id in {topic_id_list} order by rand() limit {questions_cnt}'


            result3 = await conn.execute_query_dict(query3)
            result3 = pd.DataFrame(result3)
            if result3.empty:
                return JSONResponse(status_code=400,
                                    content={"response": "insufficent data or something wrong", "success": False})
            subject_id_list = result3['subject_id'].unique()
            if len(subject_id_list) > 1:
                subject_id_list = tuple(subject_id_list)
            elif len(subject_id_list) == 1:
                subject_id_list = subject_id_list[0]
                subject_id_list = "(" + str(subject_id_list) + ")"
            result3 = result3.fillna(0)
            result3 = result3.to_dict('records')

            # Subject List by exam ID
            query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={exam_id} and subject_id in {subject_id_list} group by exam_subjects.subject_id'
            subject_list = await conn.execute_query_dict(query)
            response = {"time_allowed": int(time_allowed), "Subjects": subject_list, "questions_list": result3,
                        "success": True}
            print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))
            return JSONResponse(status_code=200,
                                content=response)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error":f"{e}","success":False})