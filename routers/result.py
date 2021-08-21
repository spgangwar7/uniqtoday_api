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
from tortoise.models import Model
from tortoise.transactions import in_transaction
from tabulate import tabulate
from schemas.SaveResult import SaveResult
router = APIRouter(
    prefix='/api',
    tags=['Student Save Result'],
)
def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True

@router.post('/student-save-result', description='Save result', status_code=201)
async def save_result(data:SaveResult,background_tasks: BackgroundTasks):
    try:
        conn = Tortoise.get_connection("default")
        getJson = jsonable_encoder(data)
        df_j = pd.DataFrame([getJson])
        user_id = int(df_j['user_id'].iloc[0])
        test_time = (df_j['test_time'].iloc[0])
        time_taken = (df_j['time_taken'].iloc[0])
        class_id = (df_j['class_id'].iloc[0])
        all_questions_list =(df_j['questions_list'].iloc[0])
        no_of_question = int(df_j['no_of_question'].iloc[0])
        answerList = df_j['answerList'].iloc[0]
        # Getting exam name i.e question_bank_jee
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

        Query = f"SELECT qtable.question_id, qtable.subject_id,qtable.chapter_id,qtable.topic_id, subjects.subject_name, qtable.marks, qtable.negative_marking, template_type, answers,question_options \
            FROM {classTablename} qtable \
            LEFT JOIN subjects ON subjects.id = qtable.subject_id\
            WHERE question_id IN {tuple(all_questions_list)}\
            ORDER BY  subject_id"

        Question_attemt_record = await conn.execute_query_dict(Query)
        Question_attemt_recorddf = pd.DataFrame(Question_attemt_record)
        Question_attemt_recorddf=Question_attemt_recorddf.fillna(0)
        #print(Question_attemt_recorddf)
        Question_attemt_record=Question_attemt_recorddf.set_index('question_id')
        answerList_copy = answerList.copy()

        total_correctAttempt = 0
        total_incorrectAttempt = 0
        marks_gain = 0
        ans_swap_count = 0
        for val in answerList_copy:
            gain_mark = 0
            quesId = (int(val['question_id']))
            #print(quesId)
            # Taking out coorect answers
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
            #print(correctOptDict)
            ## Taking out Marks and Negative Marks
            marks = Question_attemt_record['marks'].loc[quesId]
            negative_marking = Question_attemt_record['negative_marking'].loc[quesId]
            ### Taking attempted count
            attemptCount = int(val['attemptCount'])
            ans_swap_count = ans_swap_count + attemptCount

            # checking correct attemplted or not
            ## for correct attempt:1 and incorrect_attemot:0
            correct_attempt, incorrect_attempt = 0, 0
            getAnswer = str(int(val['answer']))
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
            val.update({'question_id': quesId, 'subject_id': subject_id, 'topic_id': topic_id,'chapter_id':chapter_id, 'gain_mark': gain_mark,
                        "attempt_correct": correct_attempt, 'attemtpt_incorrect_cnt': incorrect_attempt, "attempt_cnt": 1,
                        "marks": marks, "negative_marking": negative_marking})
            #print(quesId, 'gain_mark=', gain_mark, ";", "attempt_cnt=1;", "attempt_correct=", correct_attempt,'incorrect_attempt=', incorrect_attempt)

        # Inserting on db user_results
        answerList_copydf=pd.DataFrame(answerList_copy)
        pd.options.display.max_columns = None
        pd.options.display.width = None
        #print(answerList_copydf)
        unattmepted_ques_cnt = no_of_question - (total_correctAttempt + total_incorrectAttempt)
        total_exam_marks = int(answerList_copy[0]["marks"]) * no_of_question
        result_percentage = int(round((marks_gain / int(total_exam_marks)) * 100))
        if result_percentage < 0: result_percentage = 0
        query_update = f"INSERT INTO user_result (user_id,class_grade_id,no_of_question, correct_ans, incorret_ans, unattmepted_ques_cnt, marks_gain, test_time, time_taken, result_percentage, ans_swap_count ) \
                        VALUES ({user_id},{class_id},{no_of_question},{total_correctAttempt},{total_incorrectAttempt},{unattmepted_ques_cnt},{marks_gain},'{test_time}','{time_taken}', {result_percentage}, {ans_swap_count} )"
        qryExecute=await conn.execute_query(query_update)
        if not qryExecute:
            qryExecute=0
        else:
            qryExecute=1

        resultId = 0000
        # getting result_id
        if qryExecute == 1:
            query_resultId = "SELECT * FROM user_result ORDER BY id DESC LIMIT 1"
            resultId = await conn.execute_query_dict(query_resultId)
            df_resultId=pd.DataFrame(resultId)
            resultId = int(df_resultId.iloc[0]['id'])
        # Inserting on Db student_questions_attempted for each of ques
        unattempted_questions_list = all_questions_list.copy()
        for quesDict in answerList_copy:
            question_id = int(quesDict['question_id'])
            answer={"Answer:":quesDict['answer']}
            answer=json.dumps(answer)
            if question_id in unattempted_questions_list: unattempted_questions_list.remove(question_id)
            subject_id = int(quesDict['subject_id'])
            chapter_id = int(quesDict['chapter_id'])
            topic_id = int(quesDict['topic_id'])
            attempt_cnt = int(quesDict['attempt_cnt'])
            attempt_correct = int(quesDict['attempt_correct'])
            attempt_incorrect_cnt = int(quesDict['attemtpt_incorrect_cnt'])
            question_marks = int(quesDict['marks'])
            gain_marks = int(quesDict['gain_mark'])
            time_taken_sec = str(quesDict['timetaken'])
            answer_swap_cnt = int(quesDict['attemptCount'])
            if attempt_correct==1:

                qry_update = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,student_result_id,subject_id,chapter_id,topic_id,question_id,question_marks,gain_marks,time_taken,answer_swap_cnt,attempt_status,option_id) \
                               VALUES ({class_id},{user_id},{resultId},{subject_id},{chapter_id},{topic_id},{question_id},{question_marks}, {gain_marks}, '{time_taken_sec}',{answer_swap_cnt},'Correct','{answer}')"
                await conn.execute_query_dict(qry_update)
            else:
                qry_update = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,student_result_id,subject_id,chapter_id,topic_id,question_id,question_marks,gain_marks,time_taken,answer_swap_cnt,attempt_status,option_id) \
                                               VALUES ({class_id},{user_id},{resultId},{subject_id},{chapter_id},{topic_id},{question_id},{question_marks}, {gain_marks}, '{time_taken_sec}',{answer_swap_cnt},'Incorrect','{answer}' )"
                await conn.execute_query_dict(qry_update)
        # inserting for unattempted quest
        for unattemptQues in unattempted_questions_list:
            subject_id = Question_attemt_record.loc[unattemptQues]['subject_id']
            topic_id = Question_attemt_record.loc[unattemptQues]['topic_id']
            question_marks = int(answerList_copy[0]['marks'])
            qry_update2 = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,student_result_id,subject_id, chapter_id ,topic_id,question_id,question_marks,gain_marks,negative_marks_cnt,time_taken,answer_swap_cnt,attempt_status) \
                               VALUES ({class_id},{user_id},{resultId},{subject_id},{chapter_id},{topic_id},{unattemptQues},{question_marks}, 0, 0, '00:00:00',0,'Unanswered')"
            await conn.execute_query_dict(qry_update2)

        #Background task to insert record in student_performance_summary table

        background_tasks.add_task(save_student_summary, user_id,class_id)

        #Get stats of result by subject
        subjectquery=f'SELECT sqa.subject_id, subjects.subject_name, count(*) as total_questions,sum(attempt_status="Correct") as correct_count, sum(attempt_status="Incorrect") as incorrect_count,sum(attempt_status="Unanswered") as unanswered_count FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id  where student_result_id={resultId} group by subject_id;'
        resultbysubject=await conn.execute_query_dict(subjectquery)
        #Get stats of result by topic
        topicquery = f'SELECT sqa.subject_id,subjects.subject_name ,topic_id ,topics.topic_name,  count(*) as total_questions,sum(attempt_status="Correct") as correct_count, sum(attempt_status="Incorrect") as incorrect_count,sum(attempt_status="Unanswered") as unanswered_count FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id left join topics on sqa.topic_id =topics.id where student_result_id={resultId} group by subject_id,topic_id order by subject_id;'
        resultbytopic = await conn.execute_query_dict(topicquery)
        resultbytopic=pd.DataFrame(resultbytopic)
        resultbytopic=resultbytopic.fillna("")
        resultbytopic=resultbytopic.to_dict("records")

        #Graph of class average  and student subject wise score
        query = f'SELECT sqa.subject_id, (sum(attempt_status="Correct")/count(attempt_status)*100) as student_score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} and class_exam_id={class_id} group by subject_id;'
        student_score = await conn.execute_query_dict(query)
        student_score=pd.DataFrame(student_score)

        classquery = f'SELECT sqa.subject_id,(sum(attempt_status="Correct")/count(attempt_status)*100) as class_score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where class_exam_id={class_id} group by subject_id;'
        class_score = await conn.execute_query_dict(classquery)
        class_score=pd.DataFrame(class_score)
        subjectslist={}
        if r.exists(str(class_id) + "_examid"):
            exam_cache = json.loads(r.get(str(class_id) + "_examid"))
            if "subjectslist" in exam_cache:
                subjectslist = exam_cache['subjectslist']
            else:
                query3 = f'SELECT subject_id,subject_name FROM exam_subjects as es inner join subjects on es.subject_id=subjects.id where class_exam_id={class_id}'
                subjectslist = await conn.execute_query_dict(query3)
                exam_cache['subjectslist']=subjectslist
                r.set(str(class_id) + "_examid", json.dumps(exam_cache))

        subjectslist=pd.DataFrame(subjectslist)
        scoredf=pd.merge(subjectslist, class_score, on='subject_id',how="left")
        scoredf=pd.merge(scoredf, student_score, on='subject_id',how="left")
        scoredf=scoredf.fillna(0)

        scoredf = scoredf.astype({"class_score": float, "student_score": float})
        query1 = f"SELECT DISTINCT(user_id),(result_percentage),created_at as test_date FROM user_result WHERE class_grade_id={class_id}  group by created_at ORDER BY result_percentage DESC "

        val1 = await conn.execute_query_dict(query1)
        df = pd.DataFrame(val1)
        df.rename(columns={"result_percentage": "score"}, inplace=True)
        df.index += 1
        df = df.drop_duplicates(['user_id'])
        topten = df.head(10)
        v = df[df['user_id'] == user_id]['score'].max()
        a = df['user_id'].unique()
        i, = np.where(a == user_id)
        resp = {
            "no_of_question": int(no_of_question),
            "correct_count": int(total_correctAttempt),
            "correct_score": int(answerList_copy[0]["marks"] * total_correctAttempt),
            "wrong_count": int(total_incorrectAttempt),
            "incorrect_score": int(answerList_copy[0]["negative_marking"] * total_incorrectAttempt),
            "total_exam_marks": int(total_exam_marks),
            "total_get_marks": marks_gain,
            "result_time_taken": time_taken,
            "not_answered": len(list(map(int,unattempted_questions_list))),
            "result_percentage": result_percentage,
            "result_id": int(resultId),
            "subject_wise_result":resultbysubject,
            "topic_wise_result": resultbytopic,
            "subject_graph":scoredf.to_dict(orient='records'),
            "total_participants": int(len(a)),
            "user_rank": int(i[0] + 1),
            "success":True

            }
        #result=pd.DataFrame([resp])
        return resp
        #return JSONResponse(status_code=200,content=result.to_json(orient='records',date_format='iso'))

    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={"error": f"{e}","success":False})

@router.post('/save-student-summary', description='Save Student summary', status_code=201)
async def save_student_summary(student_id:int,exam_id:int):
    try:
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        conn = Tortoise.get_connection("default")
        query = f'SELECT * FROM student_questions_attempted where student_id={student_id} and class_exam_id={exam_id}'
        result= await conn.execute_query_dict(query)
        resultdf=pd.DataFrame(result)
        if resultdf.empty:
            return JSONResponse(status_code=400,content={"response":"invalid credentials","success":False})
        print(len(resultdf))
        #Initializing Redis
        r = redis.Redis()
        exam_cache={}
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

        for result_dict in result:
            joinquery = f"SELECT question_id,unit_id,skill_id,difficulty_level,major_concept_id FROM {classTablename} where question_id={result_dict['question_id']}"
            mergeoutput = await conn.execute_query_dict(joinquery)
            for mergeoutputdict in mergeoutput:
                if not mergeoutput:
                    print("Question id not found:"+str(result_dict['question_id']))
                else:
                    minoridquery = f"SELECT * FROM question_concepts where question_id={result_dict['question_id']}"
                    minorid = await conn.execute_query_dict(minoridquery)
                    result2.append(mergeoutputdict)

        result2=pd.DataFrame(result2)
        #print(result2)
        final_df = pd.merge(resultdf, result2, how='inner', on='question_id')
        final_df=final_df.loc[:, final_df.columns != 'time_taken'].fillna(0)
        dfgrouponehot = pd.get_dummies(final_df, columns=['attempt_status'], prefix=['attempt_status'])
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
        print(len(newdf.index))
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
            created_on=dict['created_on'].strftime('%Y-%m-%d')
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