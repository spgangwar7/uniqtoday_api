import traceback
from http import HTTPStatus
import numpy as np
import pandas as pd
import redis
from tortoise import Tortoise
from tortoise.queryset import QuerySet
from fastapi import APIRouter,HTTPException
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from tortoise.query_utils import Q
from fastapi.responses import JSONResponse
import json
from datetime import datetime
from db.sparkSession import spark_session

router = APIRouter(
    prefix='/api/analytics',
    tags=['Student Analytics'],
)


def td_to_str(td):
    """
    convert a timedelta object td to a string in HH:MM:SS format.
    """
    hours, remainder = divmod(td.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{int(hours):02}:{int(minutes):02}:{int(seconds):02}'

@router.get('/overall-analytics/{user_id}',description='Overall analytics for student',status_code=201)
async def overall_analytics(user_id:int=0):
    try:
        start_time = datetime.now()
        conn = Tortoise.get_connection('default')
        query = f'select id,test_type,exam_mode,marks_gain,result_percentage from user_result where user_id={user_id} order by id desc limit 2'
        test_score = await conn.execute_query_dict(query)

        student_cache={}

        #Initializing Redis
        r = redis.Redis()
        if r.exists(str(user_id)+"_sid"):
            student_cache= json.loads(r.get(str(user_id)+"_sid"))
            if "exam_id" in student_cache:
                class_exam_id = student_cache['exam_id']
            else:
                query = f'SELECT class_exam_id FROM student_questions_attempted where student_id={user_id} limit 1'  # fetch exam_id by user_id
                class_exam_id = await conn.execute_query_dict(query)
                if len(class_exam_id) == 0:
                    resp = {
                        "message": "No exam Found for this user",
                        "success": False
                    }
                    return resp, 400
                class_exam_id = int(class_exam_id[0]['class_exam_id'])

                student_cache['exam_id']=class_exam_id
                r.set(str(user_id) + "_sid", json.dumps(student_cache))
        else:
            query = f'SELECT class_exam_id FROM student_questions_attempted where student_id={user_id} limit 1'  # fetch exam_id by user_id
            class_exam_id = await conn.execute_query_dict(query)
            if len(class_exam_id) == 0:
                resp = {
                    "message": "No exam Found for this user",
                    "success": False
                }
                return resp, 400
            class_exam_id = int(class_exam_id[0]['class_exam_id'])

            student_cache={"exam_id":class_exam_id}
            r.set(str(user_id) + "_sid", json.dumps(student_cache))

        query1 = f"SELECT DISTINCT(user_id),(result_percentage),created_at as test_date FROM user_result WHERE class_grade_id={class_exam_id} group by created_at ORDER BY result_percentage DESC "


        val1 = await conn.execute_query_dict(query1)
        query2 = f'SELECT sqa.subject_id,subject_name, count(attempt_status) as total_questions,sum(attempt_status="Correct") as correct_ans,sum(attempt_status="Incorrect") as incorrect_ans,sum(attempt_status="Unanswered") as unanswered,(sum(attempt_status="Correct")/count(attempt_status)*100) as score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} group by subject_id;'
        val2 = await conn.execute_query_dict(query2)
        subjectslist={}

        if r.exists(str(class_exam_id) + "_examid"):
            exam_cache = json.loads(r.get(str(class_exam_id) + "_examid"))
            if "subjectslist" in exam_cache:
                subjectslist = exam_cache['subjectslist']
            else:
                query3 = f'SELECT subject_id,subject_name FROM exam_subjects as es inner join subjects on es.subject_id=subjects.id where class_exam_id={class_exam_id}'
                subjectslist = await conn.execute_query_dict(query3)
                exam_cache['subjectslist']=subjectslist
                r.set(str(class_exam_id) + "_examid", json.dumps(exam_cache))

        for subjectslist_dict in subjectslist:
            if not any(d['subject_id'] == subjectslist_dict['subject_id'] for d in val2):
                val2.append(
                    {'subject_id': subjectslist_dict['subject_id'], 'subject_name': subjectslist_dict['subject_name'],
                     'total_questions': 0, 'correct_ans': 0,'incorrect_ans': 0,'unanswered': 0, 'score': 0})


        val2=pd.DataFrame(val2)
        df = pd.DataFrame(val1)
        df.rename(columns={"result_percentage":"score"},inplace=True)
        df.index+=1
        df=df.drop_duplicates(['user_id'])
        v=df[df['user_id']==user_id]['score'].max()
        a = df['user_id'].unique()
        i, = np.where(a == user_id)


        #finding marks trend
        query5 = f'SELECT marks_gain,created_at as test_date FROM user_result where DATE(created_at) >= DATE(NOW()) - INTERVAL 28 DAY and user_id={user_id} and class_grade_id={class_exam_id}'
        result = await conn.execute_query_dict(query5)
        resultdf = pd.DataFrame(result)
        output = resultdf.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.max().to_json(orient='records',date_format='iso')
        query6 = f'SELECT user_id,marks_gain,created_at as test_date FROM user_result where DATE(created_at) >= DATE(NOW()) - INTERVAL 28 DAY and class_grade_id={class_exam_id};'
        result2 = await conn.execute_query_dict(query6)
        resultdf2 = pd.DataFrame(result2)
        output1 = resultdf2.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.mean().to_json(orient='records',date_format='iso')
        output2 = resultdf2.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.max().to_json(orient='records',date_format='iso')

        # find daily progress

        query=f"SELECT updated_at AS date,no_of_question,IFNULL(correct_ans,0) AS correct_ans," \
              f"IFNULL(incorret_ans,0)AS incorrect_ans,(no_of_question-(IFNULL(correct_ans,0)+IFNULL(incorret_ans,0))) AS unattempted_questions,test_time,time_taken FROM user_result" \
              f" WHERE user_id = {user_id} GROUP BY DATE"


        data=await conn.execute_query_dict(query)
        data=pd.DataFrame(data)
        data['time_spent_on_each_question'] = (data['time_taken'] / data['no_of_question'])
        data['time_spent_on_correct_ques'] = data['time_spent_on_each_question'] * data['correct_ans']
        data['time_spent_on_incorrect_ques'] = data['time_spent_on_each_question'] * data['incorrect_ans']
        data['correct_percentage']=(data['correct_ans']/data['no_of_question'])*100
        data['incorrect_percentage']=(data['incorrect_ans']/data['no_of_question'])*100


        #for all users in exam

        query1=f"SELECT updated_at AS date,no_of_question,IFNULL(correct_ans,0) AS correct_ans," \
              f"IFNULL(incorret_ans,0)AS incorrect_ans,(no_of_question-(IFNULL(correct_ans,0)+IFNULL(incorret_ans,0))) AS unattempted_questions,test_time,time_taken FROM user_result" \
              f" WHERE class_grade_id = {class_exam_id} GROUP BY DATE"


        data1=await conn.execute_query_dict(query1)
        data1=pd.DataFrame(data1)
        data1['time_spent_on_each_question_for_class']=(data1['time_taken']/data1['no_of_question'])
        data1['time_spent_on_correct_ques_for_class']=data1['time_spent_on_each_question_for_class']*data1['correct_ans']
        data1['time_spent_on_incorrect_ques_for_class']=data1['time_spent_on_each_question_for_class']*data1['incorrect_ans']
        data1['correct_percentage_for_class'] = (data1['correct_ans'] / data1['no_of_question']) * 100
        data1['incorrect_percentage_for_class'] = (data1['incorrect_ans'] / data1['no_of_question']) * 100

         #concat two data frames into one
        data=pd.concat([data,data1['time_spent_on_each_question_for_class'],data1['time_spent_on_correct_ques_for_class'],data1['time_spent_on_incorrect_ques_for_class'],data1['correct_percentage_for_class'],data1['incorrect_percentage_for_class']],axis=1,join='inner')

        # find weakly Report
        weaklyReport=data.groupby(pd.Grouper(freq='W',key='date')).\
            agg({"correct_ans":sum,"incorrect_ans":sum,"unattempted_questions":sum,
                 "no_of_question":sum,"time_spent_on_each_question":sum,"time_spent_on_correct_ques":sum,
                 "time_spent_on_incorrect_ques":sum,"time_spent_on_correct_ques_for_class":sum,
                 "time_spent_on_incorrect_ques_for_class":sum,"time_spent_on_each_question_for_class":sum,
                 "correct_percentage":sum,"incorrect_percentage":sum,"correct_percentage_for_class":sum,
                 "incorrect_percentage_for_class":sum})

        weaklyReport["time_spent_on_each_question"] = weaklyReport["time_spent_on_each_question"].apply(td_to_str)
        weaklyReport["time_spent_on_correct_ques"] = weaklyReport["time_spent_on_correct_ques"].apply(td_to_str)
        weaklyReport["time_spent_on_incorrect_ques"] = weaklyReport["time_spent_on_incorrect_ques"].apply(td_to_str)
        weaklyReport["time_spent_on_correct_ques_for_class"] = weaklyReport["time_spent_on_correct_ques_for_class"].apply(td_to_str)
        weaklyReport["time_spent_on_incorrect_ques_for_class"] = weaklyReport["time_spent_on_incorrect_ques_for_class"].apply(td_to_str)
        weaklyReport["time_spent_on_each_question_for_class"] = weaklyReport["time_spent_on_each_question_for_class"].apply(td_to_str)
        weaklyReport = weaklyReport.reset_index()
        weaklyReport["date"] = pd.to_datetime(weaklyReport["date"]).dt.strftime('%d-%m-%Y')
        #weaklyReport["weakly_time_spent_on_correct_ques"] = pd.to_datetime(weaklyReport["weakly_time_spent_on_correct_ques"]).dt.strftime('%H:%M:%S')
        #print(weaklyReport)


        #find monthly report

        #monthlyReport
        monthlyReport = data.groupby(pd.Grouper(freq='M', key='date')).\
            agg({"correct_ans":sum,"incorrect_ans":sum,"unattempted_questions":sum,
                 "no_of_question":sum,"time_spent_on_each_question":sum,"time_spent_on_correct_ques":sum,
                 "time_spent_on_incorrect_ques":sum,"time_spent_on_correct_ques_for_class":sum,
                 "time_spent_on_incorrect_ques_for_class":sum,"time_spent_on_each_question_for_class":sum,
                 "correct_percentage":sum,"incorrect_percentage":sum,"correct_percentage_for_class":sum,"incorrect_percentage_for_class":sum})

        monthlyReport["time_spent_on_each_question"] = monthlyReport["time_spent_on_each_question"].apply(td_to_str)
        monthlyReport["time_spent_on_correct_ques"] = monthlyReport["time_spent_on_correct_ques"].apply(td_to_str)
        monthlyReport["time_spent_on_incorrect_ques"] = monthlyReport["time_spent_on_incorrect_ques"].apply(td_to_str)
        monthlyReport["time_spent_on_correct_ques_for_class"] = monthlyReport["time_spent_on_correct_ques_for_class"].apply(td_to_str)
        monthlyReport["time_spent_on_incorrect_ques_for_class"] = monthlyReport["time_spent_on_incorrect_ques_for_class"].apply(td_to_str)
        monthlyReport["time_spent_on_each_question_for_class"] = monthlyReport["time_spent_on_each_question_for_class"].apply(td_to_str)
        monthlyReport = monthlyReport.reset_index()
        monthlyReport["date"] = pd.to_datetime(monthlyReport["date"]).dt.strftime('%d-%m-%Y')

        # dailyReport
        data["date"] = pd.to_datetime(data["date"]).dt.strftime('%d-%m-%Y')
        data['test_time'] = data['test_time'].apply(td_to_str)
        data["time_taken"]=data["time_taken"].apply(td_to_str)
        data['time_spent_on_each_question']=data["time_spent_on_each_question"].apply(td_to_str)
        data["time_spent_on_correct_ques"]=data["time_spent_on_correct_ques"].apply(td_to_str)
        data['time_spent_on_incorrect_ques']=data['time_spent_on_incorrect_ques'].apply(td_to_str)
        data["time_spent_on_each_question_for_class"]=data['time_spent_on_each_question_for_class'].apply(td_to_str)
        data['time_spent_on_correct_ques_for_class']=data["time_spent_on_correct_ques_for_class"].apply(td_to_str)
        data['time_spent_on_incorrect_ques_for_class']=data['time_spent_on_incorrect_ques_for_class'].apply(td_to_str)



        student_accuracy_query=f'SELECT DAYNAME(created_on)  as dateDay,(sum(attempt_status="Correct")/count(attempt_status)*100) as student_accuracy FROM student_questions_attempted WHERE created_on BETWEEN TIMESTAMPADD(Month, -1, NOW()) AND NOW() and student_id={user_id} group by dateDay;'
        student_accuracy=await conn.execute_query_dict(student_accuracy_query)
        student_accuracy=pd.DataFrame(student_accuracy)
        class_accuracy_query=f'SELECT DAYNAME(created_on)  as dateDay,(sum(attempt_status="Correct")/count(attempt_status)*100) as class_accuracy FROM student_questions_attempted WHERE created_on BETWEEN TIMESTAMPADD(Month, -1, NOW()) AND NOW() group by dateDay;'
        class_accuracy=await conn.execute_query_dict(class_accuracy_query)
        class_accuracy=pd.DataFrame(class_accuracy)
        accuracydf=pd.merge(class_accuracy, student_accuracy, on='dateDay',how="left")
        accuracydf=accuracydf.fillna(0)

        student_time_taken_q=f'SELECT DAYNAME(created_on) as dateDay,avg(time_taken) as student_time_taken FROM student_questions_attempted WHERE created_on BETWEEN TIMESTAMPADD(Month, -1, NOW()) AND NOW() and student_id={user_id}  group by  dateDay;'
        student_time_taken=await conn.execute_query_dict(student_time_taken_q)
        student_time_taken=pd.DataFrame(student_time_taken)
        class_time_taken_q=f'SELECT DAYNAME(created_on) as dateDay,avg(time_taken) as class_time_taken FROM student_questions_attempted WHERE created_on BETWEEN TIMESTAMPADD(Month, -1, NOW()) AND NOW() group by dateDay;'
        class_time_taken=await conn.execute_query_dict(class_time_taken_q)
        class_time_taken=pd.DataFrame(class_time_taken)
        timetakendf=pd.merge(class_time_taken, student_time_taken, on='dateDay',how="left")
        timetakendf=timetakendf.fillna(0)

        resp = {
            "test_score":test_score,
            "total_participants": int(len(a)),
            "user_id":user_id,
            "user_rank": int(i[0]+1),
            "score": int(v),
            #"Top 10":topten.to_json(orient='index'),
            "subject_proficiency":val2.to_json(orient='records',date_format='iso'),
            "daily_report":data.to_json(orient='records',date_format='iso'),
            "weekly_report":weaklyReport.to_json(orient='records',date_format='iso'),
            "monthlyReport":monthlyReport.to_json(orient='records',date_format='iso'),
            "accuracy":accuracydf.to_json(orient='records'),
            "time_taken":timetakendf.to_json(orient='records'),
            "success":True


        }
        print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))

        return resp,200
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return {"error":f"{e}","success":False},400



"""
@router.get('/subject-analytics_score/{user_id}', description='Get Subject Wise Student Analytics', status_code=201)
async def get_subject_analytics_score(user_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query2 = f'SELECT grade_id FROM student_users where id={user_id} limit 1;'
        class_exam_id = await conn.execute_query_dict(query2)
        print(class_exam_id)
        class_exam_id = [class_exam_id_dict['grade_id'] for class_exam_id_dict in class_exam_id]
        query = f'SELECT sqa.subject_id,sqa.student_result_id,subject_name, count(attempt_status) as total_questions,sum(attempt_status="Correct") as correct_ans,(sum(attempt_status="Correct")/count(attempt_status)*100) as score,date(created_on) as test_date FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} and class_exam_id={class_exam_id[0]} group by subject_id,date(created_on);'
        subject_score = await conn.execute_query_dict(query)
        query3 = f'SELECT subject_id,subject_name FROM exam_subjects as es inner join subjects on es.subject_id=subjects.id where class_exam_id={class_exam_id[0]}'
        subjectslist = await conn.execute_query_dict(query3)
        subject_score_df=pd.DataFrame(subject_score)
        subject_score_df=subject_score_df.sort_values('test_date')
        subject_score_df = subject_score_df.astype({col: 'int' for col in subject_score_df.select_dtypes('int64').columns})
        subject_score_df = subject_score_df.astype({"score": float, "correct_ans": float,"test_date":str})
        subject_id_array=subject_score_df.subject_id.unique()
        subject_id_array=subject_id_array.tolist()
        subjectslist=[next(iter(d.values())) for d in subjectslist]
        subject_dict_final=[]
        for subject_id in subject_id_array:
            if subject_id in subjectslist:
                subjectslist.remove(subject_id)
            subject_df=subject_score_df.loc[subject_score_df['subject_id'] == subject_id].tail(2)
            subject_df_dict=subject_df.to_dict('records')

            subject_dict_final.append({"subject_id":subject_id,"scores":subject_df_dict})
        for subject_id in subjectslist:
            subject_dict_final.append({"subject_id":subject_id,"scores":0})
        return {"subject_score":subject_dict_final}
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=HTTPStatus.BAD_REQUEST)


@router.get('/subject-analytics-score/{user_id}/{subject_id}', description='Get Subject Wise Student Analytics', status_code=201)
async def get_subject_analytics_score(user_id:int=0,subject_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        scorequery = f'SELECT sqa.subject_id,sqa.student_result_id,subject_name, count(attempt_status) as total_questions,sum(attempt_status="Correct") as correct_ans,(sum(attempt_status="Correct")/count(attempt_status)*100) as score,date(created_on) as test_date FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} and sqa.subject_id={subject_id} group by student_result_id order by test_date desc limit 2'
        score=await conn.execute_query_dict(scorequery)
        for dict in score:
            for key in dict:
                dict["score"] = int(dict["score"])
                dict["correct_ans"] = int(dict["correct_ans"])
        if score:
            return {"subject_score":score}
        else :
            return {"subject_score":0}
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=HTTPStatus.BAD_REQUEST)

@router.get('/subject-analytics-skillpercentage/{user_id}/{subject_id}', description='Get Subject Wise Student Analytics', status_code=201)
async def get_subject_analytics_skillpercentage(user_id:int=0,subject_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        skillquery = f'SELECT sqa.subject_id,qbm.skill_id, count(*) as count, skills.skill_name FROM student_questions_attempted as sqa inner join question_bank_master as qbm on sqa.question_id=qbm.question_id left join skills on qbm.skill_id=skills.skill_id where student_id={user_id} and sqa.subject_id={subject_id} group by qbm.skill_id'
        skillresult=await conn.execute_query_dict(skillquery)
        skillresultdf=pd.DataFrame(skillresult)
        total=skillresultdf['count'].sum()
        skillresultdf['percentage']=skillresultdf['count']/total*100
        skillresultdf=skillresultdf.round(2)
        skillresultdf=skillresultdf.fillna("")
        output=skillresultdf.to_dict("records")

        if skillresult:
            return {"skill_percentage":output}
        else :
            return {"skill_percentage":0}
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=HTTPStatus.BAD_REQUEST)

@router.get('/subject-analytics-topicwise-proficiency/{user_id}/{subject_id}', description='Get Topic Wise Student Proficiency', status_code=201)
async def get_subject_analytics_topicwise_proficiency(user_id:int=0,subject_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        topicwisequery = f'SELECT sqa.subject_id,sqa.topic_id ,topic_name, count(attempt_status) as total_questions,sum(attempt_status="Correct") as correct_ans,(sum(attempt_status="Correct")/count(attempt_status)*100) as score  FROM student_questions_attempted as sqa  join topics on sqa.topic_id=topics.id where student_id={user_id} and sqa.subject_id={subject_id} group by topic_id'
        topicwiseresult=await conn.execute_query_dict(topicwisequery)

        if topicwiseresult:
            return {"topic_wise_result":topicwiseresult}
        else :
            return {"topic_wise_result":0}
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=HTTPStatus.BAD_REQUEST)
"""
@router.get("/subject-wise-analytics/{user_id}/{subject_id}",description='subject wise analytics',status_code=201)
async def SubjectWiseAnalytics(user_id:int=0,subject_id:int=0):
    try:
        start_time = datetime.now()
        conn = Tortoise.get_connection('default')  # get db_connection
        #Subject Score Block
        scorequery = f'SELECT sqa.subject_id,sqa.student_result_id,subject_name, count(attempt_status) as total_questions,sum(attempt_status="Correct") as correct_ans,(sum(attempt_status="Correct")/count(attempt_status)*100) as score,date(created_on) as test_date FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} and sqa.subject_id={subject_id} group by student_result_id order by test_date desc limit 2'
        #print(scorequery)
        score = await conn.execute_query_dict(scorequery)
        score=pd.DataFrame(score)
        if score.empty:
            score=0
        else:
            score['test_date']=pd.to_datetime(score['test_date']).dt.strftime("%d-%m-%Y")
            score=score.to_dict('records')
            for dict in score:
                for key in dict:
                    dict["score"] = int(dict["score"])
                    dict["correct_ans"] = int(dict["correct_ans"])
                    dict["test_date"] = str(dict["test_date"])

        if score:
            subject_score= score
        else:
            subject_score= 0
        #topic wise score
        conn = Tortoise.get_connection("default")
        topicwisequery = f'SELECT sqa.subject_id,sqa.topic_id ,topic_name, count(attempt_status) as total_questions,sum(attempt_status="Correct") as correct_ans  ,sum(attempt_status="Incorrect") as incorrect_ans,sum(attempt_status="Unanswered") as unanswered ,(sum(attempt_status="Correct")/count(attempt_status)*100) as score  FROM student_questions_attempted as sqa  join topics on sqa.topic_id=topics.id where student_id={user_id} and sqa.subject_id={subject_id} group by topic_id'
        topicwiseresult = await conn.execute_query_dict(topicwisequery)

        if not topicwiseresult:
            topic_wise_result= 0
        else:
            topic_wise_result=topicwiseresult
        #Skill percentage Block

        #Initializing Redis
        r = redis.Redis()
        if r.exists("skills"):
            skillmasterresult= json.loads(r.get("skills"))
        else:
            skillmasterquery = f'SELECT skill_id,skill_name FROM skills;'
            skillmasterresult = await conn.execute_query_dict(skillmasterquery)
            r.set("skills", json.dumps(skillmasterresult))

        skillmasterresult=pd.DataFrame(skillmasterresult)
        skillquery = f'SELECT sqa.subject_id,qbm.skill_id, count(*) as count FROM student_questions_attempted as sqa inner join question_bank_master as qbm on sqa.question_id=qbm.question_id left join skills on qbm.skill_id=skills.skill_id where student_id={user_id} and sqa.subject_id={subject_id} group by qbm.skill_id'
        skillresult=await conn.execute_query_dict(skillquery)
        skillresultdf=pd.DataFrame(skillresult)
        if skillresultdf.empty:
            skillresult=0
        else:
            skillresultdf=pd.merge(skillmasterresult, skillresultdf, on='skill_id',how="left")
            #print(skillresultdf)
            total=skillresultdf['count'].sum()
            skillresultdf['percentage']=skillresultdf['count']/total*100
            skillresultdf=skillresultdf.round(2)
            skillresultdf = skillresultdf[skillresultdf['skill_id'].notna()]
            skillresultdf = skillresultdf[skillresultdf['skill_name'].notna()]
            skillresultdf=skillresultdf.fillna(0)

            output=skillresultdf.to_dict("records")

        if skillresult:
            skill_percentage=output
        else :
            skill_percentage = 0
        #End of Skill percentage block

        # Block for subject wise analytics graphs
        class_exam_id=0

        if r.exists(str(user_id) + "_sid"):
            student_cache = json.loads(r.get(str(user_id) + "_sid"))
            if "exam_id" in student_cache:
                class_exam_id = student_cache['exam_id']
            else:
                query = f'SELECT class_exam_id FROM student_questions_attempted where student_id={user_id} and subject_id={subject_id} limit 1'  # fetch exam_id by user_id
                class_exam_id = await conn.execute_query_dict(query)
                class_exam_id = int(class_exam_id[0]['class_exam_id'])

        print(class_exam_id)
        if class_exam_id == 0:
            query2 = f'SELECT grade_id FROM student_users where id={user_id} limit 1;'
            class_exam_id = await conn.execute_query_dict(query2)
            if not class_exam_id[0]['grade_id']:
                return {"error": "User does not have any subscription", "success": False}, 400
            class_exam_id = int(class_exam_id[0]['grade_id'])

            if r.exists(str(user_id) + "_sid"):
                student_cache = json.loads(r.get(str(user_id) + "_sid"))
                if "exam_id" in student_cache:
                    class_exam_id = student_cache['exam_id']
                else:
                    query = f'SELECT class_exam_id FROM student_questions_attempted where student_id={user_id} and subject_id={subject_id} limit 1'  # fetch exam_id by user_id
                    class_exam_id = await conn.execute_query_dict(query)
                    class_exam_id = int(class_exam_id[0]['grade_id'])
                    student_cache['exam_id']=class_exam_id
                    r.set(str(user_id) + "_sid", json.dumps(student_cache))
            #raise HTTPException(status_code=404, detail="No exam Found for this user")
            #return {"error":f"No exam Found for this user","success":False},400

        query1 = f"SELECT DISTINCT(user_id),(result_percentage),created_at as test_date FROM user_result WHERE class_grade_id={class_exam_id}  group by created_at ORDER BY result_percentage DESC "

        val1 = await conn.execute_query_dict(query1)

        query2 = f'SELECT sqa.subject_id,subject_name, count(attempt_status) as total_questions,sum(attempt_status="Correct") as correct_ans,sum(attempt_status="Incorrect") as incorrect_ans,sum(attempt_status="Unanswered") as unanswered,(sum(attempt_status="Correct")/count(attempt_status)*100) as score  FROM student_questions_attempted as sqa inner join subjects on sqa.subject_id=subjects.id where student_id={user_id} and sqa.subject_id={subject_id} group by subject_id;'
        val2 = await conn.execute_query_dict(query2)
        query3 = f'SELECT subject_id,subject_name FROM exam_subjects as es inner join subjects on es.subject_id=subjects.id where class_exam_id={class_exam_id} and subject_id={subject_id}'
        subjectslist = await conn.execute_query_dict(query3)

        for subjectslist_dict in subjectslist:
            if not any(d['subject_id'] == subjectslist_dict['subject_id'] for d in val2):
                val2.append(
                    {'subject_id': subjectslist_dict['subject_id'], 'subject_name': subjectslist_dict['subject_name'],
                     'total_questions': 0, 'correct_ans': 0, 'score': 0})

        val2 = pd.DataFrame(val2)
        df = pd.DataFrame(val1)
        df.rename(columns={"result_percentage": "score"}, inplace=True)
        df.index += 1
        df = df.drop_duplicates(['user_id'])
        #topten = df.head(10)
        v = df[df['user_id'] == user_id]['score'].max()
        if pd.isna(v):
            score=0
        else:
            score= int(v)
        a = df['user_id'].unique()
        i, = np.where(a == user_id)
        if not i:
            user_rank=0
        else:
            user_rank= int(i[0] + 1)

        print("User Rank"+str(user_rank))

        # finding marks trend
        query5 = f'SELECT marks_gain,created_at as test_date FROM user_result where DATE(created_at) >= DATE(NOW()) - INTERVAL 28 DAY and user_id={user_id} and class_grade_id={class_exam_id}'
        result = await conn.execute_query_dict(query5)
        resultdf = pd.DataFrame(result)
        if not resultdf.empty:
            output = resultdf.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.max().to_json(orient='records',
                                                                                                      date_format='iso')
        else:
            output={}
        query6 = f'SELECT user_id,marks_gain,created_at as test_date FROM user_result where DATE(created_at) >= DATE(NOW()) - INTERVAL 28 DAY and class_grade_id={class_exam_id};'
        result2 = await conn.execute_query_dict(query6)
        resultdf2 = pd.DataFrame(result2)
        output1 = resultdf2.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.mean().to_json(orient='records',
                                                                                                     date_format='iso')
        output2 = resultdf2.groupby(pd.Grouper(freq='W', key='test_date')).marks_gain.max().to_json(orient='records',
                                                                                                    date_format='iso')

        # find daily progress
        query=f'SELECT sqa.created_on AS date,s.subject_name,COUNT(sqa.attempt_status) AS no_of_question,' \
              f'IFNULL(SUM(sqa.attempt_status="Correct"),0) AS correct_ans,IFNULL(SUM(sqa.attempt_status="Incorrect"),0)' \
              f' AS incorrect_ans,(COUNT(sqa.attempt_status)-(IFNULL(SUM(sqa.attempt_status="Correct"),0)+IFNULL(SUM(sqa.attempt_status="Incorrect"),0))) AS' \
              f' unattempted_questions,time_taken FROM student_questions_attempted sqa INNER JOIN subjects s ON s.id=sqa.subject_id' \
              f' WHERE sqa.student_id = {user_id} AND sqa.subject_id={subject_id} GROUP BY date'

        data = await conn.execute_query_dict(query)
        data = pd.DataFrame(data)
        if data.empty:
            data=pd.DataFrame()
        else:
            data['time_spent_on_each_question'] = (data['time_taken'] / data['no_of_question'])

            data['time_spent_on_correct_ques'] = data['time_spent_on_each_question'] * data['correct_ans'].astype(float)
            data['time_spent_on_incorrect_ques'] = data['time_spent_on_each_question'] * data['incorrect_ans'].astype(
                float)
            data['correct_percentage'] = (data['correct_ans'] / data['no_of_question']) * 100
            data['incorrect_percentage'] = (data['incorrect_ans'] / data['no_of_question']) * 100
        # for all users in exam

        query1 = f'SELECT created_on AS date,COUNT(attempt_status) AS no_of_question,IFNULL(SUM(attempt_status="Correct"),0)' \
              f' AS correct_ans,IFNULL(SUM(attempt_status="Incorrect"),0)AS incorrect_ans,(COUNT(attempt_status)-(IFNULL(SUM(attempt_status="Correct"),0)+IFNULL(SUM(attempt_status="Incorrect"),0))) ' \
              f'AS unattempted_questions,time_taken FROM student_questions_attempted WHERE class_exam_id = {class_exam_id} AND subject_id={subject_id} GROUP BY date'

        data1 = await conn.execute_query_dict(query1)
        data1 = pd.DataFrame(data1)
        if data1.empty:
            data1=pd.DataFrame()
            weaklyReport=pd.DataFrame()
            monthlyReport=pd.DataFrame()
        else:
            data1['time_spent_on_each_question_for_class'] = (data1['time_taken'] / data1['no_of_question'])
            data1['time_spent_on_correct_ques_for_class'] = data1['time_spent_on_each_question_for_class'] * data1[
                'correct_ans'].astype(float)
            data1['time_spent_on_incorrect_ques_for_class'] = data1['time_spent_on_each_question_for_class'] * data1[
                'incorrect_ans'].astype(float)
            data1['correct_percentage_for_class'] = (data1['correct_ans'] / data1['no_of_question']) * 100
            data1['incorrect_percentage_for_class'] = (data1['incorrect_ans'] / data1['no_of_question']) * 100

            # concat two data frames into one
            data = pd.concat(
                [data, data1['time_spent_on_each_question_for_class'], data1['time_spent_on_correct_ques_for_class'],
                 data1['time_spent_on_incorrect_ques_for_class'], data1['correct_percentage_for_class'],
                 data1['incorrect_percentage_for_class']], axis=1, join='inner')
            #print(data)
        # find weakly Report
            if not data.empty:
                weakly_total_correct = data.groupby(pd.Grouper(freq='W', key='date')).correct_ans.sum()
                weakly_total_incorrect = data.groupby(pd.Grouper(freq='W', key='date')).incorrect_ans.sum()
                weakly_total_unattempted_questions = data.groupby(pd.Grouper(freq='W', key='date')).unattempted_questions.sum()
                weakly_total_no_of_question = data.groupby(pd.Grouper(freq='W', key='date')).no_of_question.sum()
                weakly_time_spent_on_each_question = data.groupby(
                    pd.Grouper(freq='W', key='date')).time_spent_on_each_question.sum().apply(td_to_str)
                weakly_time_spent_on_correct_ques = data.groupby(
                    pd.Grouper(freq='W', key='date')).time_spent_on_correct_ques.sum().apply(td_to_str)
                weakly_time_spent_on_incorrect_ques = data.groupby(
                    pd.Grouper(freq='W', key='date')).time_spent_on_incorrect_ques.sum().apply(td_to_str)
                weakly_time_spent_on_correct_ques_for_class = data.groupby(
                    pd.Grouper(freq='W', key='date')).time_spent_on_correct_ques_for_class.sum().apply(td_to_str)
                weakly_time_spent_on_incorrect_ques_for_class = data.groupby(
                    pd.Grouper(freq='W', key='date')).time_spent_on_incorrect_ques_for_class.sum().apply(td_to_str)
                weakly_time_spent_on_each_ques_for_class = data.groupby(
                    pd.Grouper(freq='W', key='date')).time_spent_on_each_question_for_class.sum().apply(td_to_str)
                weakly_correct_percentage = data.groupby(pd.Grouper(freq='W', key='date')).correct_percentage.sum()
                weakly_incorrect_percentage = data.groupby(pd.Grouper(freq='W', key='date')).incorrect_percentage.sum()
                weakly_correct_percentage_for_class = data.groupby(
                    pd.Grouper(freq='W', key='date')).correct_percentage_for_class.sum()
                weakly_incorrect_percentage_for_class = data.groupby(
                    pd.Grouper(freq='W', key='date')).incorrect_percentage_for_class.sum()

                weaklyframes = [weakly_total_correct, weakly_total_incorrect, weakly_total_unattempted_questions,
                                weakly_total_no_of_question, weakly_time_spent_on_correct_ques,
                                weakly_time_spent_on_incorrect_ques,
                                weakly_time_spent_on_correct_ques_for_class, weakly_time_spent_on_incorrect_ques_for_class,
                                weakly_time_spent_on_each_ques_for_class,
                                weakly_time_spent_on_each_question, weakly_correct_percentage, weakly_incorrect_percentage,
                                weakly_correct_percentage_for_class, weakly_incorrect_percentage_for_class]
                weaklyReport = pd.concat(weaklyframes, axis=1, join='inner')
                weaklyReport = weaklyReport.reset_index()
                weaklyReport["date"] = pd.to_datetime(weaklyReport["date"]).dt.strftime('%d-%m-%Y')

                # find monthly report

                monthly_total_correct = data.groupby(pd.Grouper(freq='M', key='date')).correct_ans.sum()
                monthly_total_incorrect = data.groupby(pd.Grouper(freq='M', key='date')).incorrect_ans.sum()
                monthly_total_unattempted_questions = data.groupby(pd.Grouper(freq='M', key='date')).unattempted_questions.sum()
                monthly_total_no_of_question = data.groupby(pd.Grouper(freq='M', key='date')).no_of_question.sum()
                monthly_time_spent_on_each_question = data.groupby(
                    pd.Grouper(freq='M', key='date')).time_spent_on_each_question.sum().apply(td_to_str)
                monthly_time_spent_on_correct_ques = data.groupby(
                    pd.Grouper(freq='M', key='date')).time_spent_on_correct_ques.sum().apply(td_to_str)
                monthly_time_spent_on_incorrect_ques = data.groupby(
                    pd.Grouper(freq='M', key='date')).time_spent_on_incorrect_ques.sum().apply(td_to_str)
                monthly_time_spent_on_correct_ques_for_class = data.groupby(
                    pd.Grouper(freq='M', key='date')).time_spent_on_correct_ques_for_class.sum().apply(td_to_str)
                monthly_time_spent_on_incorrect_ques_for_class = data.groupby(
                    pd.Grouper(freq='M', key='date')).time_spent_on_incorrect_ques_for_class.sum().apply(td_to_str)
                monthly_time_spent_on_each_ques_for_class = data.groupby(
                    pd.Grouper(freq='M', key='date')).time_spent_on_each_question_for_class.sum().apply(td_to_str)
                monthly_correct_percentage = data.groupby(pd.Grouper(freq='M', key='date')).correct_percentage.sum()
                monthly_incorrect_percentage = data.groupby(pd.Grouper(freq='M', key='date')).incorrect_percentage.sum()
                monthly_correct_percentage_for_class = data.groupby(
                    pd.Grouper(freq='M', key='date')).correct_percentage_for_class.sum()
                monthly_incorrect_percentage_for_class = data.groupby(
                    pd.Grouper(freq='M', key='date')).incorrect_percentage_for_class.sum()

                monthlyFrames = [monthly_total_correct, monthly_total_incorrect, monthly_total_unattempted_questions,
                                 monthly_total_no_of_question, monthly_time_spent_on_each_question,
                                 monthly_time_spent_on_correct_ques, monthly_time_spent_on_incorrect_ques,
                                 monthly_time_spent_on_correct_ques_for_class, monthly_time_spent_on_incorrect_ques_for_class,
                                 monthly_time_spent_on_each_ques_for_class, monthly_correct_percentage,
                                 monthly_incorrect_percentage,
                                 monthly_correct_percentage_for_class, monthly_incorrect_percentage_for_class]
                monthlyReport = pd.concat(monthlyFrames, axis=1, join='inner')
                monthlyReport = monthlyReport.reset_index()
                monthlyReport["date"] = pd.to_datetime(monthlyReport["date"]).dt.strftime('%d-%m-%Y')
                data["date"] = pd.to_datetime(data["date"]).dt.strftime('%d-%m-%Y')
                data["time_taken"] = data["time_taken"].apply(td_to_str)
                data['time_spent_on_each_question'] = data["time_spent_on_each_question"].apply(td_to_str)
                data["time_spent_on_correct_ques"] = data["time_spent_on_correct_ques"].apply(td_to_str)
                data['time_spent_on_incorrect_ques'] = data['time_spent_on_incorrect_ques'].apply(td_to_str)
                data["time_spent_on_each_question_for_class"] = data['time_spent_on_each_question_for_class'].apply(td_to_str)
                data['time_spent_on_correct_ques_for_class'] = data["time_spent_on_correct_ques_for_class"].apply(td_to_str)
                data['time_spent_on_incorrect_ques_for_class'] = data['time_spent_on_incorrect_ques_for_class'].apply(td_to_str)
            else:
                weaklyReport = pd.DataFrame()
                monthlyReport = pd.DataFrame()

        class_accuracy_query = f'SELECT DAYNAME(created_on)  as dateDay,(sum(attempt_status="Correct")/count(attempt_status)*100) as class_accuracy FROM student_questions_attempted WHERE created_on BETWEEN TIMESTAMPADD(Month, -1, NOW()) AND NOW() group by dateDay;'
        class_accuracy = await conn.execute_query_dict(class_accuracy_query)
        class_accuracy = pd.DataFrame(class_accuracy)
        student_accuracy_query=f'SELECT DAYNAME(created_on)  as dateDay,(sum(attempt_status="Correct")/count(attempt_status)*100) as student_accuracy FROM student_questions_attempted WHERE created_on BETWEEN TIMESTAMPADD(Month, -1, NOW()) AND NOW() and student_id={user_id} group by dateDay;'
        student_accuracy=await conn.execute_query_dict(student_accuracy_query)
        student_accuracy=pd.DataFrame(student_accuracy)
        if student_accuracy.empty:
            student_accuracy=pd.DataFrame()
            accuracydf=pd.DataFrame()
        else:

            accuracydf=pd.merge(class_accuracy, student_accuracy, on='dateDay',how="left")
            accuracydf=accuracydf.fillna(0)

        student_time_taken_q=f'SELECT DAYNAME(created_on) as dateDay,avg(time_taken) as student_time_taken FROM student_questions_attempted WHERE subject_id={subject_id} and created_on BETWEEN TIMESTAMPADD(Month, -1, NOW()) AND NOW() and student_id={user_id}  group by  dateDay;'
        student_time_taken=await conn.execute_query_dict(student_time_taken_q)
        student_time_taken=pd.DataFrame(student_time_taken)
        class_time_taken_q=f'SELECT DAYNAME(created_on) as dateDay,avg(time_taken) as class_time_taken FROM student_questions_attempted WHERE subject_id={subject_id} and created_on BETWEEN TIMESTAMPADD(Month, -1, NOW()) AND NOW() group by dateDay;'
        class_time_taken=await conn.execute_query_dict(class_time_taken_q)
        class_time_taken=pd.DataFrame(class_time_taken)
        #print(student_time_taken)
        if not student_time_taken.empty:
            timetakendf = pd.merge(class_time_taken, student_time_taken, on='dateDay', how="left")
            timetakendf = timetakendf.fillna(0)
        else:
            timetakendf=class_time_taken
            timetakendf['student_time_taken']=0
        if data.empty:
            daily_report={}
        else:
            daily_report=data.to_json(orient='records', date_format='iso')
        if not weaklyReport.empty:
            weaklyReport=weaklyReport.to_json(orient='records', date_format='iso')
        if not monthlyReport.empty:
            monthlyReport=monthlyReport.to_json(orient='records', date_format='iso')
        if not accuracydf.empty:
            accuracydf=accuracydf.to_json(orient='records', date_format='iso')
        resp = {
            "total_participants": int(len(a)),
            "user_id": user_id,
            "user_rank": user_rank,
            "score": score,
            "subject_score": subject_score,
            "topic_wise_result":topic_wise_result,
            "skill_percentage": skill_percentage,
            # "Top 10":topten.to_json(orient='index'),
            "subject_proficiency": val2.to_json(orient='records', date_format='iso'),
            "daily_report": daily_report,
            "weekly_report": weaklyReport,
            "monthlyReport": monthlyReport,
            "accuracy": accuracydf,
            "time_taken": timetakendf.to_json(orient='records'),
            "success":True

        }
        print("Time took for execution for this API: %s seconds " % (datetime.now() - start_time))

        return resp,200
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return {"error":f"{e}","success":False},400
"""
@router.get("/spark-sample",description='',status_code=201)
async def Spark_Sample():
    print("Inside spark sample")
    df=spark_session("user_result")
    print(df.show(5))
"""