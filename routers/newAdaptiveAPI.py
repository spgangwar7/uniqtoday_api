import traceback
from http import HTTPStatus
from typing import List

import numpy as np
import pandas as pd
from fastapi import APIRouter
import redis
import json
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
from datetime import datetime,timedelta
from schemas.AdaptiveQuestionsNew import AdaptiveQuestionsMock,AdaptiveQuestionsMock2,AdaptiveQuestionsChapterPractice,AdaptiveQuestionsTopicPractice

router = APIRouter(
    prefix='/api',
    tags=['Adaptive API New'],
)

@router.get('/update-priority-sequence', description='Update priority and sequence in db', status_code=201)
async def updatePriority():
    filepath=f'./models/import.csv'
    dataframe=pd.read_csv(filepath)
    for index, row in dataframe.iterrows():
        topic_id=row['topic_id']
        topic_sequence=row['topic_sequence']
        topic_priority=row['topic_priority']
        chapter_id=row['chapter_id']
        updatequery=f'update topics SET topic_priority={topic_priority},topic_sequence={topic_sequence} where ' \
                    f'chapter_id={chapter_id} and id={topic_id}'
        conn = Tortoise.get_connection("default")
        result=await conn.execute_query_dict(updatequery)
    return "Data updated successfully in table"

def getrank(rank:int,answer:str,trend:str,lastbase:int):
    base_list=[1,5,10,14,19,23]
    base_list_desc=[5,9,14,18,23,27]
    if answer=="correct":
        if trend=="":
            if rank<27:
                if rank in base_list:
                    rank=rank+4
                else:
                    rank=rank+1
                trend="asc"
                lastbase=rank
        elif trend=="asc":
            trend="asc"
            if rank<27:
                if lastbase>rank:
                    rank=rank+1
                elif rank>=lastbase:
                    if rank in base_list:
                        rank=rank+4
                        lastbase=rank
                    else:
                        rank=rank+1
        elif trend=="desc":
            trend="asc"
            if rank<27:
                if lastbase>rank:
                    rank=rank+1
                elif rank==lastbase:
                    lastbase=rank
                    if rank in base_list:
                        rank=rank+4
                    else:
                        rank=rank+1
    elif answer=="wrong":
        trend="desc"
        if rank>1:
            if rank in base_list_desc:
                rank=rank-4
            else:
                rank=rank-1
    print("Rank: "+str(rank)+" Last Base: "+str(lastbase)+" trend: "+trend)
    return rank,trend,lastbase

@router.post('/adaptive-assessment-chapter-mock-exam', description='Get Adaptive Questions for mock test on chapter level', status_code=201)
async def getAdaptiveMockTest(input:AdaptiveQuestionsMock):
    try:
        exam_id=input.exam_id
        student_id=input.student_id
        subject_id=input.subject_id
        chapter_id=input.chapter_id
        exam_type_id=input.exam_type_id
        test_type_id=input.test_type_id
        conn = Tortoise.get_connection("default")
        check_topic_id = []
        questions = []
        query1 = f'select question_bank_name,questions_cnt,simple_ques_cnt,medium_ques_cnt,complex_ques_cnt,marks_cnt,time_in_min from ' \
                f'test_pattern_setup where id={test_type_id}'
        result = await conn.execute_query_dict(query1)
        result=result[0]
        query2 = f'select id, topic_priority,subject_id from topics where class_id={exam_id} and chapter_id={chapter_id}'
        topics = await conn.execute_query_dict(query2)
        topics_df=pd.DataFrame(topics)
        topics_df = topics_df.sort_values(by=['topic_priority'], ascending=True)
        total_topics=len(topics_df.index)
        print(topics_df)

        no_of_question=result['questions_cnt']
        cutt_of1 = result['complex_ques_cnt']
        cutt_of2 = result['medium_ques_cnt']
        cutt_of3 = result['simple_ques_cnt']
        
        if total_topics == 0:
            return f"No topics for given chapter : {chapter_id} in exam: {exam_id}"
        if total_topics == no_of_question:
            print("case1")
            count1 = 0
            count2 = 0
            count3 = 0
            question_Query = f'SELECT question_id,qb.topic_id,difficulty_level,topics.topic_priority FROM question_bank_jee' \
                             f' as qb left join topics on qb.topic_id=topics.id where qb.chapter_id={chapter_id}'
            question = await conn.execute_query_dict(question_Query)
            question=pd.DataFrame(question)
            question['difficulty_level'] = np.where(
                (question['difficulty_level'] <= 9) & (question['difficulty_level'] > 1), 1, question['difficulty_level']
            )
            question['difficulty_level'] = np.where(
                (question['difficulty_level'] <= 18) & (question['difficulty_level'] > 10), 2, question['difficulty_level']
            )
            question['difficulty_level'] = np.where(
                (question['difficulty_level'] <= 27) & (question['difficulty_level'] > 19), 3, question['difficulty_level']
            )
            question = question.sort_values(by=['topic_priority','difficulty_level'], ascending=True)
            """print(question['topic_id'].unique().tolist())
            print(question.loc[(question['difficulty_level'] == 1)])
            print(question.loc[(question['difficulty_level'] == 2)])
            print(question.loc[(question['difficulty_level'] == 3)])
            """
            question=question.to_dict('records')
            if total_topics == 0:
                return f"No topics for given chapter : {chapter_id} in exam: {exam_id}"
            if total_topics == no_of_question:
                print("case1")
                count1 = 0
                count2 = 0
                count3 = 0
                for data in question:
                    # print(data)
                    if data['topic_id'] not in check_topic_id:
                        if data['difficulty_level'] == 3 and count1 < cutt_of1:
                            count1 = count1 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                        elif data['difficulty_level'] == 2 and count2 < cutt_of2:
                            count2 = count2 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                        elif data['difficulty_level'] == 1 and count3 < cutt_of3:
                            count3 = count3 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                return questions
            elif total_topics > no_of_question:
                print("case2")
                count1 = 0
                count2 = 0
                count3 = 0
                count = no_of_question
                for data in question:
                    if data['topic_id'] not in check_topic_id and count != 0:
                        count = count - 1
                        if data['difficulty_level'] == 3 and count1 < cutt_of1:
                            count1 = count1 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                        elif data['difficulty_level'] == 2 and count2 < cutt_of2:
                            count2 = count2 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                        elif data['difficulty_level'] == 1 and count3 < cutt_of3:
                            count3 = count3 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                return questions
            elif total_topics > no_of_question:
                print("case2")
                count1 = 0
                count2 = 0
                count3 = 0
                count = no_of_question
                for data in question:
                    if data['topic_id'] not in check_topic_id and count != 0:
                        count = count - 1
                        if data['difficulty_level'] == 3 and count1 < cutt_of1:
                            count1 = count1 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                        elif data['difficulty_level'] == 2 and count2 < cutt_of2:
                            count2 = count2 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                        elif data['difficulty_level'] == 1 and count3 < cutt_of3:
                            count3 = count3 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)

            elif total_topics < no_of_question:
                print("case3")

                count1 = 0
                count2 = 0
                count3 = 0
                count = no_of_question
                for data in question:
                    if data['topic_id'] not in check_topic_id and count != 0:
                        count = count - 1
                        if data['difficulty_level'] == 3 and count1 < cutt_of1:
                            count1 = count1 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                        elif data['difficulty_level'] == 2 and count2 < cutt_of2:
                            count2 = count2 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                        elif data['difficulty_level'] == 1 and count3 < cutt_of3:
                            count3 = count3 + 1
                            check_topic_id.append(data['topic_id'])
                            dict1 = {
                                "question_id": data['question_id'],
                                "difficulty_level": data['difficulty_level'],
                                "topic_id": data['topic_id'],
                                "priority": data['topic_priority']
                            }
                            questions.append(dict1)
                return questions

                count = no_of_question
                check_question_id = []
                while (count != 0):
                    for data in res:
                        if data['topic_id'] not in check_topic_id and count != 0 and data[
                            'question_id'] not in check_question_id:
                            count = count - 1
                            if data['difficulty_level'] == 3 and count1 < cutt_of1:
                                count1 = count1 + 1
                                check_topic_id.append(data['topic_id'])
                                check_question_id.append(data['question_id'])
                                dict1 = {
                                    "question_id": data['question_id'],
                                    "difficulty_level": data['difficulty_level'],
                                    "topic_id": data['topic_id'],
                                    "priority": data['topic_priority']
                                }
                                questions.append(dict1)
                            elif data['difficulty_level'] == 2 and count2 < cutt_of2:
                                count2 = count2 + 1
                                check_topic_id.append(data['topic_id'])
                                check_question_id.append(data['question_id'])
                                dict1 = {
                                    "question_id": data['question_id'],
                                    "difficulty_level": data['difficulty_level'],
                                    "topic_id": data['topic_id'],
                                    "priority": data['topic_priority']
                                }
                                questions.append(dict1)
                            elif data['difficulty_level'] == 1 and count3 < cutt_of3:
                                count3 = count3 + 1
                                check_topic_id.append(data['topic_id'])
                                check_question_id.append(data['question_id'])
                                dict1 = {
                                    "question_id": data['question_id'],
                                    "difficulty_level": data['difficulty_level'],
                                    "topic_id": data['topic_id'],
                                    "priority": data['topic_priority']
                                }
                                questions.append(dict1)
                    check_topic_id.clear()
                return questions

            return questions

        resp={
            'message': "Class/Exam List",
            'response': result,
            "success":True
        }
        return JSONResponse(status_code=200,content=resp)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400,content={'error': f'{e}',"success":False})

@router.post('/adaptive-assessment-subject-mock-exam', description='Get Adaptive Questions for mock test on chapter level', status_code=201)
async def getAdaptiveMockTest(input:AdaptiveQuestionsMock2):
    try:
        exam_id=input.exam_id
        student_id=input.student_id
        conn = Tortoise.get_connection("default")
        check_topic_id = []
        questions = []
        query1 = f'select subject_id,question_bank_name,questions_cnt,marks_cnt,time_in_min from ' \
                f'test_pattern_setup where class_id={exam_id} and test_name="Assessment"'
        result = await conn.execute_query_dict(query1)
        print(pd.DataFrame(result))
        for subject_dict in result:
            print(subject_dict)
            subject_id=subject_dict['subject_id']
        #result=result[0]
        """
        query2 = f'select id, topic_priority,subject_id from topics where class_id={exam_id} and chapter_id={chapter_id}'
        topics = await conn.execute_query_dict(query2)
        topics_df=pd.DataFrame(topics)
        topics_df = topics_df.sort_values(by=['topic_priority'], ascending=True)
        total_topics=len(topics_df.index)
        """
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}', "success": False})


def getQuestionsTrack3(input:dict,topics,new:str,student_id:int):
    count=1
    initial_rank=14
    student_cache={}
    r = redis.Redis()
    if r.exists(str(student_id) + "_track3"):
        count=int(r.get(str(student_id) + "_track3"))
    else:
        count = 1
        r.setex(str(student_id) + "_track3", timedelta(minutes=10), count)
        # print("Student Data stored in redis")

    if new == "yes":
        count = 1
        r.setex(str(student_id) + "_track3", timedelta(minutes=10), count)
        topicsdf = pd.DataFrame(topics)
        topicsdf['rank'] = initial_rank
        topicsdf['lastbase'] = initial_rank
        topicsdf['trend'] = ""
        topic_lastrow = topicsdf.iloc[-0]
        result = topic_lastrow.to_dict()
        return result
    if new == "no":
        if input:
            input=input[0]
            print("*Getting questions for next set")
            if count < 2:
                rank = input['rank']
                answer = input['answer']
                trend = input['trend']
                lastbase = input['lastbase']
                rank, trend, lastbase = getrank(rank, answer, trend, lastbase)
                input['rank'] = rank
                input['trend'] = trend
                input['lastbase'] = lastbase
                count = count + 1
                r.setex(str(student_id) + "_track3", timedelta(minutes=30), count)
                result = input
                return result
            if count >= 2:
                priority = input['topic_priority']
                topicsdf = pd.DataFrame(topics)
                topic_lastrow = topicsdf.iloc[-1]
                topic_lastrow = topic_lastrow.to_dict()
                maxpriority = topic_lastrow['topic_priority']
                if priority < maxpriority:
                    priority = priority + 1
                else:
                    priority = 1
                outputList = topicsdf[topicsdf['topic_priority'].isin([priority])]
                outputList = outputList.to_dict("records")
                outputList = outputList[0]
                rank = input['rank']
                answer = input['answer']
                trend = input['trend']
                lastbase = input['lastbase']
                rank, trend, lastbase = getrank(rank, answer, trend, lastbase)
                outputList['rank'] = rank
                outputList['trend'] = trend
                outputList['lastbase'] = lastbase
                count = 1
                r.setex(str(student_id) + "_track3", timedelta(minutes=10), count)
                return outputList

def getQuestionsTrack2(input:list,topics,new:str):
    initial_rank = 10
    if new=="yes":
        topicsdf=pd.DataFrame(topics)
        topicsdf['rank']=initial_rank
        topicsdf['lastbase']=initial_rank
        topicsdf['trend'] = ""
        topic_lastrow=topicsdf.iloc[-1]
        topic_lastrow=topic_lastrow.to_dict()
        maxpriority=topic_lastrow['topic_priority']
        outputdf=topicsdf.iloc[:2]
        for index, row in outputdf.iterrows():
            print(f"**Getting questions for topic : {row['topic_id']} and rank {row['rank']}")
        result=outputdf.to_dict("records")
        return result
    if new=="no":
            if input:
                print("*Getting questions for next set")
                topicsdf=pd.DataFrame(topics)
                topicsdf['rank']=initial_rank
                topic_lastrow=topicsdf.iloc[-1]
                topic_lastrow=topic_lastrow.to_dict()
                maxpriority=topic_lastrow['topic_priority']
                inputdf=pd.DataFrame(input)
                lastrow=inputdf.iloc[-1]
                lastrow=lastrow.to_dict()
                lastpriority=lastrow['topic_priority']
                rank=lastrow['rank']
                trend=lastrow['trend']
                lastbase=lastrow['lastbase']
                answersList=inputdf['answer'].tolist()
                print(f'lastpriority :{lastpriority} maxpriority:{maxpriority}')
                if answersList.count("correct")==2:
                    print("Increase rank")
                    rank,trend,lastbase=getrank(rank,"correct",trend,lastbase)
                if answersList.count("wrong")==2:
                    print("Decrease rank")
                    rank,trend,lastbase=getrank(rank,"wrong",trend,lastbase)
                    print(lastbase)
                if answersList.count("wrong")==1:
                    print("Rank not changed, 1 question each of correct and wrong received")
                if lastpriority<=maxpriority-2:
                    outputList=topicsdf[topicsdf['topic_priority'].isin([lastpriority+1, lastpriority+2])]
                    outputList=outputList[['topic_id','topic_priority']]
                    outputList['rank']=rank
                    outputList['trend']=trend
                    outputList['lastbase']=lastbase
                    result=outputList.to_dict("records")
                    return result
                if lastpriority<=maxpriority-1:
                    outputList=topicsdf[topicsdf['topic_priority'].isin([1,maxpriority])]
                    outputList=outputList[['topic_id','topic_priority']]
                    outputList['rank']=rank
                    outputList['trend']=trend
                    outputList['lastbase']=lastbase
                    result=outputList.to_dict("records")
                    return result
                if lastpriority==maxpriority:
                    outputList=topicsdf[topicsdf['topic_priority'].isin([1,2])]
                    outputList=outputList[['topic_id','topic_priority']]
                    outputList['rank']=rank
                    outputList['trend']=trend
                    outputList['lastbase']=lastbase
                    result=outputList.to_dict("records")
                    return result

def getQuestionstrack1(input:list,topics,new:str):
    print("Getting questions as per Track 1")
    initial_rank = 14
    if new=="yes":
        print("*Getting questions for 1st set")
        topicsdf=pd.DataFrame(topics)
        topicsdf['rank']=initial_rank
        topicsdf['lastbase'] = initial_rank
        topicsdf['trend'] = ""
        result=topicsdf.to_dict("records")
        return result
    if new=="no":
        if input:
            print("*Getting questions for next set")
            topicsdf=pd.DataFrame(input)
            for index, row in topicsdf.iterrows():
                rank=row['rank']
                answer=row['answer']
                trend=row['trend']
                lastbase=row['lastbase']
                rank,trend,lastbase=getrank(rank,answer,trend,lastbase)
                row['rank']=rank
                row['trend']=trend
                row['lastbase']=lastbase
                topicsdf.iloc[index] = row
            result=topicsdf.to_dict("records")
            return result
        else:
            print("*Previous set state is not available, Getting questions as per 1st set")
            topicsdf=pd.DataFrame(topics)
            topicsdf['rank']=initial_rank
            topicsdf['lastbase'] = initial_rank
            topicsdf['trend'] = ""
            for index, row in topicsdf.iterrows():
                print(f"**Getting questions for topic : {row['topic_id']} and rank {row['rank']}")
            result=topicsdf.to_dict("records")
            return result

@router.post('/adaptive-assessment-chapter-practice', description='Get Adaptive Questions for chapter practice', status_code=201)
async def getAdaptiveQuestions(input:AdaptiveQuestionsChapterPractice):
    try:
        exam_id=input.exam_id
        student_id=input.student_id
        chapter_id=input.chapter_id
        session_id=input.session_id
        answerList=input.answerList
        marks_gain = 0
        ans_swap_count = 0
        exam_time_per_ques=1
        conn = Tortoise.get_connection("default")
        check_topic_id = []
        questions = []
        questions_list=[]
        track=""
        query2 = f'select id as topic_id, topic_priority from topics where class_id={exam_id} and chapter_id={chapter_id}'
        topics = await conn.execute_query_dict(query2)
        topics_df = pd.DataFrame(topics)
        topics_df = topics_df.sort_values(by=['topic_priority'], ascending=True)
        total_topics = len(topics_df.index)
        quiz_bank=""
        all_questions_list_str=""
        all_questions_list=input.questions_list
        if len(all_questions_list) == 1:
            all_questions_list_str = "(" + str(all_questions_list[0]) + ")"
        else:
            all_questions_list_str = tuple(all_questions_list)

        r = redis.Redis()
        if r.exists(str(student_id)+"_sid"):
            student_cache= json.loads(r.get(str(student_id)+"_sid"))
            #print("Redis student data: "+str(student_cache))
            if "quiz_bank" in student_cache:
                quiz_bank = student_cache['quiz_bank']
                #print(quiz_bank)
            else:
                query = f'Select question_bank_name from class_exams where id ={exam_id}'
                df_quiz1 = await conn.execute_query_dict(query)
                df_quiz = pd.DataFrame(df_quiz1)
                quiz_bank = df_quiz['question_bank_name'].iloc[0]
                student_cache['quiz_bank']=quiz_bank
                r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))
        else:
            query = f'Select question_bank_name from class_exams where id ={exam_id}'
            df_quiz1 = await conn.execute_query_dict(query)
            df_quiz = pd.DataFrame(df_quiz1)
            quiz_bank = df_quiz['question_bank_name'].iloc[0]
            student_cache={"exam_id":exam_id,"quiz_bank":quiz_bank}
            r.setex(str(student_id)+"_sid", timedelta(days=1), json.dumps(student_cache))
            #print("Student Data stored in redis")


        if session_id==0:
            #check last track and get new track
            #If user is new he gets track1 in 1st attempt
            tracks = ["track1", "track2", "track3"]
            track_check = f'select track from adaptive_session where student_id={student_id} and track is not null order by createdAt desc limit 1'
            track_list = await conn.execute_query_dict(track_check)
            if track_list:
                print(track_list)
                track = track_list[0]
                track=track['track']
                tracks.remove(track)
                import random
                track = random.choice(tracks)
                "Selecting random track"
            else:
                track = "track1"
            print(f"Track: {track}")


            query=f'INSERT INTO adaptive_session (student_id,track) VALUES ({student_id},"{track}"); '
            await conn.execute_query_dict(query)
            session_id= await conn.execute_query_dict('select LAST_INSERT_ID() as session_id;')
            session_id=session_id[0]['session_id']

            if track == "track1":
                result = getQuestionstrack1([], topics, "yes")
            if track == "track2":
                result = getQuestionsTrack2([], topics, "yes")
            if track == "track3":
                result_dict = getQuestionsTrack3([], topics, "yes",student_id)
                result=[result_dict]
            result_df=pd.DataFrame(result)
            #print(result_df)
            for topics_dict in result:
                topic_id=topics_dict['topic_id']
                rank=topics_dict['rank']
                questions_query=f'select question_id,topic_id,difficulty_level as difficulty_level from {quiz_bank} where topic_id={topic_id} and difficulty_level={rank} limit 1'
                question_id=await conn.execute_query_dict(questions_query)
                if question_id:
                    questions.append(question_id[0])
            questions_df=pd.DataFrame(questions)
            output_df=pd.merge(result_df, questions_df, on='topic_id', how='inner')
            print(output_df)

            adaptiveState=output_df.to_dict("records")
            for state in adaptiveState:
                t_id=state['topic_id']
                t_priority=state['topic_priority']
                rank=state['difficulty_level']
                lastbase=state['lastbase']
                trend=state['trend']
                #print(state['topic_id'])
                statequery=f"INSERT INTO adaptive_student_states (chapter_id,topic_id,topic_priority,adaptive_rank,lastbase,trend,session_id)" \
                      f"VALUES ({chapter_id},{t_id},{t_priority},{rank},{lastbase},'{trend}',{session_id})"
                await conn.execute_query_dict(statequery)
            questions_list=output_df['question_id'].to_list()

        else:
            track_check = f'select track from adaptive_session where adaptive_session_id={session_id}'
            track_list = await conn.execute_query_dict(track_check)
            if track_list:
                print(track_list)
                track = track_list[0]
                track=track['track']
            else:
                return JSONResponse(status_code=400, content={'msg': f"Track does not exist for this session id."
                                                                     f"Kindly start a new test", "success": False})
            print("Get session details")
            state_query = f'select chapter_id,topic_id,topic_priority,adaptive_rank as `rank`,lastbase,trend,session_id from adaptive_student_states where session_id={session_id}'
            adaptive_state = await conn.execute_query_dict(state_query)
            adaptive_state_df=pd.DataFrame(adaptive_state)
            #print(adaptive_state_df)

            Query = f"SELECT question_id, subject_id,topic_id,chapter_id, marks,negative_marking," \
                    f" template_type,answers,question_options \
                FROM {quiz_bank} WHERE question_id IN {all_questions_list_str}"
            Question_attemt_record = await conn.execute_query_dict(Query)
            Question_attemt_recorddf = pd.DataFrame(Question_attemt_record)
            Question_attemt_recorddf = Question_attemt_recorddf.fillna(0)
            Question_attemt_record = Question_attemt_recorddf.set_index('question_id')
            answerList_copy = answerList.copy()
            #print(Question_attemt_record)
            new_answer_list = []
            for val in answerList_copy:
                dict = {}
                gain_mark = 0
                quesId = int(val.question_id)
                template_type = Question_attemt_record['template_type'].loc[quesId]
                if template_type == 3:
                    correctAnswervalue = (Question_attemt_record['answers'].loc[quesId]).strip('\"')
                    # print(correctAnswervalue)
                    question_options = (Question_attemt_record['question_options'].loc[quesId])
                    question_options = json.loads(question_options)
                    for key, value in question_options.items():
                        if correctAnswervalue == value:
                            correctAnswer = value
                else:
                    correctOptDict = (Question_attemt_record['answers'].loc[quesId])
                    #print(correctOptDict)
                    correctOptDict = json.loads(correctOptDict)
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
                else:
                    incorrect_attempt = 1
                    marks_gain = int(marks_gain + negative_marking)
                    gain_mark = int(negative_marking)

                subject_id = Question_attemt_record['subject_id'].loc[quesId]
                chapter_id = Question_attemt_record['chapter_id'].loc[quesId]
                topic_id = Question_attemt_record['topic_id'].loc[quesId]
                dict = {'question_id': quesId, 'subject_id': subject_id, "chapter_id": chapter_id, 'topic_id': topic_id,
                        'gain_mark': gain_mark,
                        "attempt_correct": correct_attempt, 'attemtpt_incorrect_cnt': incorrect_attempt,
                        "attempt_cnt": 1,
                        "marks": marks, "negative_marking": negative_marking}
                dict.update(jsonable_encoder(val))
                new_answer_list.append(dict)
            #print(pd.DataFrame(new_answer_list))
            new_answer_list_df=pd.DataFrame(new_answer_list)
            new_answer_list_df=new_answer_list_df[['topic_id','attempt_correct']]
            #print(new_answer_list_df)
            output_df=pd.merge(adaptive_state_df, new_answer_list_df, on='topic_id', how='inner')
            output_df['answer'] = np.where(output_df['attempt_correct'] == 1, 'correct', 'wrong')
            #output_df['attempt_correct'] = output_df['attempt_correct'].replace(['1'], 'correct')
            #print(output_df)
            if track =="track1":
                result = getQuestionstrack1(output_df.to_dict("records"), topics, "no")
            if track == "track2":
                result = getQuestionsTrack2(output_df.to_dict("records"), topics, "no")
            if track == "track3":
                result_dict = getQuestionsTrack3(output_df.to_dict("records"), topics, "no",student_id)
                result=[result_dict]

            result_df = pd.DataFrame(result)
            #print(result_df)
            for topics_dict in result:
                topic_id=topics_dict['topic_id']
                rank=topics_dict['rank']
                questions_query=f'select question_id,topic_id,difficulty_level as difficulty_level from {quiz_bank} where topic_id={topic_id} and difficulty_level={rank} limit 1'
                question_id=await conn.execute_query_dict(questions_query)
                if question_id:
                    questions.append(question_id[0])
            questions_df=pd.DataFrame(questions)
            questions_df2=pd.merge(result_df, questions_df, on='topic_id', how='inner')
            print(questions_df2)
            ##Save adaptive state in database

            adaptiveState = questions_df2.to_dict("records")
            for state in adaptiveState:
                t_id = state['topic_id']
                t_priority = state['topic_priority']
                rank = state['difficulty_level']
                lastbase = state['lastbase']
                trend = state['trend']
                # print(state['topic_id'])
                state_select_query=f"select id from adaptive_student_states where session_id={session_id} and topic_id={t_id}"
                state_id=await conn.execute_query_dict(state_select_query)
                if state_id:
                    statequery = f"UPDATE adaptive_student_states SET chapter_id={chapter_id},topic_id={t_id},topic_priority={t_priority}" \
                             f",adaptive_rank={rank},lastbase={lastbase},trend='{trend}' where session_id={session_id} and topic_id={t_id}"
                else:
                    statequery=f"INSERT INTO adaptive_student_states (chapter_id,topic_id,topic_priority,adaptive_rank,lastbase,trend,session_id)" \
                      f"VALUES ({chapter_id},{t_id},{t_priority},{rank},{lastbase},'{trend}',{session_id})"
                await conn.execute_query_dict(statequery)

            # Inserting on Db student_questions_attempted for each of ques
            unattempted_questions_list = all_questions_list.copy()
            for quesDict in new_answer_list:
                question_id = int(quesDict['question_id'])
                answer = {"Answer:": quesDict['answer']}
                answer = json.dumps(answer)
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
                if attempt_correct == 1:

                    qry_update = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,time_taken,answer_swap_cnt,attempt_status,option_id,session_id) \
                                   VALUES ({exam_id},{student_id},{subject_id},{chapter_id},{topic_id},'PE',{question_id},{question_marks}, {gain_marks}, '{time_taken_sec}',{answer_swap_cnt},'Correct','{answer}','{session_id}')"
                    await conn.execute_query_dict(qry_update)
                else:
                    qry_update = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,negative_marks_cnt,time_taken,answer_swap_cnt,attempt_status,option_id,session_id) \
                                                   VALUES ({exam_id},{student_id},{subject_id},{chapter_id},{topic_id},'PE',{question_id},{question_marks}, {gain_marks},1,'{time_taken_sec}',{answer_swap_cnt},'Incorrect','{answer}','{session_id}' )"
                    await conn.execute_query_dict(qry_update)
            for unattemptQues in unattempted_questions_list:
                chapter_id = Question_attemt_record.loc[unattemptQues]['chapter_id']
                subject_id = Question_attemt_record.loc[unattemptQues]['subject_id']
                topic_id = Question_attemt_record.loc[unattemptQues]['topic_id']
                question_marks = int(new_answer_list[0]['marks'])
                qry_insert2 = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,negative_marks_cnt,time_taken,answer_swap_cnt,attempt_status,session_id) \
                                   VALUES ({exam_id},{student_id},{subject_id},{chapter_id},{topic_id},'PE',{unattemptQues},{question_marks}, 0, 0, '00:00:00',0,'Unanswered','{session_id}')"
                await conn.execute_query_dict(qry_insert2)
            questions_list=questions_df2['question_id'].to_list()
            print(f"questions list: {questions_list}")
        if questions_list:
            total_time = exam_time_per_ques * len(questions_list)

            if len(questions_list) == 1:
                question_list_str = "(" + str(questions_list[0]) + ")"
            else:
                # print(questions_list)
                question_list_str = tuple(questions_list)

            query = f'select qb.question_id, qb.subject_id,qb.chapter_id, qb.topic_id , qb.difficulty_level, qb.question, qb.template_type, \
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

            response = {"time_allowed": l1,"session_id":session_id, "questions": l2, "success": True}
            jsonstr = json.dumps(l2, ensure_ascii=False).encode('utf8')
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
        return JSONResponse(status_code=400, content={'error': f'{e}', "success": False})

@router.post('/adaptive-assessment-topic-practice', description='Get Adaptive Questions for Topic practice', status_code=201)
async def getAdaptiveQuestions(input:AdaptiveQuestionsTopicPractice):
    try:
        exam_id=input.exam_id
        student_id=input.student_id
        topic_id=input.topic_id
        session_id=input.session_id
        answerList=input.answerList
        marks_gain = 0
        ans_swap_count = 0
        exam_time_per_ques=1
        conn = Tortoise.get_connection("default")
        check_topic_id = []
        questions = []
        questions_list=[]
        quiz_bank=""
        all_questions_list_str=""
        all_questions_list=input.questions_list

        r = redis.Redis()
        if r.exists(str(student_id)+"_sid"):
            student_cache= json.loads(r.get(str(student_id)+"_sid"))
            #print("Redis student data: "+str(student_cache))
            if "quiz_bank" in student_cache:
                quiz_bank = student_cache['quiz_bank']
                #print(quiz_bank)
            else:
                query = f'Select question_bank_name from class_exams where id ={exam_id}'
                df_quiz1 = await conn.execute_query_dict(query)
                df_quiz = pd.DataFrame(df_quiz1)
                quiz_bank = df_quiz['question_bank_name'].iloc[0]
                student_cache['quiz_bank']=quiz_bank
                r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))
        else:
            query = f'Select question_bank_name from class_exams where id ={exam_id}'
            df_quiz1 = await conn.execute_query_dict(query)
            df_quiz = pd.DataFrame(df_quiz1)
            quiz_bank = df_quiz['question_bank_name'].iloc[0]
            student_cache={"exam_id":exam_id,"quiz_bank":quiz_bank}
            r.setex(str(student_id)+"_sid", timedelta(days=1), json.dumps(student_cache))

        if len(all_questions_list) == 1:
            all_questions_list_str = "(" + str(all_questions_list[0]) + ")"
        else:
            all_questions_list_str = tuple(all_questions_list)
        if session_id==0:
            initial_rank=14
            rank = initial_rank
            lastbase = initial_rank
            trend = ""
            query=f'INSERT INTO adaptive_session (student_id) VALUES ({student_id}); '
            await conn.execute_query_dict(query)
            session_id= await conn.execute_query_dict('select LAST_INSERT_ID() as session_id;')
            session_id=session_id[0]['session_id']

            questions_query = f'select question_id,topic_id,difficulty_level as difficulty_level from {quiz_bank} where topic_id={topic_id} and difficulty_level={rank} limit 1'
            question_id = await conn.execute_query_dict(questions_query)
            if question_id:
                questions_list.append(question_id[0]['question_id'])
            else:
                questions_query = f'select question_id,topic_id,difficulty_level as difficulty_level from {quiz_bank} where topic_id={topic_id} and difficulty_level<={rank} limit 1'
                question_id = await conn.execute_query_dict(questions_query)
                rank=question_id[0]['difficulty_level']
                questions_list.append(question_id[0]['question_id'])

            statequery = f"INSERT INTO adaptive_student_states (topic_id,adaptive_rank,lastbase,trend,session_id)" \
                         f"VALUES ({topic_id},{rank},{lastbase},'{trend}',{session_id})"
            await conn.execute_query_dict(statequery)

        else:
            print("Get session details")
            state_query = f'select topic_id,adaptive_rank as `rank`,lastbase,trend,session_id from adaptive_student_states where session_id={session_id}'
            adaptive_state = await conn.execute_query_dict(state_query)
            adaptive_state=adaptive_state[0]
            print(adaptive_state)


            #section for save result and validation of answer
            Query = f"SELECT question_id, subject_id,topic_id,chapter_id, marks,negative_marking," \
                    f" template_type,answers,question_options \
                FROM {quiz_bank} WHERE question_id IN {all_questions_list_str}"
            Question_attemt_record = await conn.execute_query_dict(Query)
            Question_attemt_recorddf = pd.DataFrame(Question_attemt_record)
            Question_attemt_recorddf = Question_attemt_recorddf.fillna(0)
            Question_attemt_record = Question_attemt_recorddf.set_index('question_id')
            answerList_copy = answerList.copy()
            #print(Question_attemt_record)
            new_answer_list = []
            marks=1
            for val in answerList_copy:
                dict = {}
                gain_mark = 0
                quesId = int(val.question_id)
                template_type = Question_attemt_record['template_type'].loc[quesId]
                if template_type == 3:
                    correctAnswervalue = (Question_attemt_record['answers'].loc[quesId]).strip('\"')
                    # print(correctAnswervalue)
                    question_options = (Question_attemt_record['question_options'].loc[quesId])
                    question_options = json.loads(question_options)
                    for key, value in question_options.items():
                        if correctAnswervalue == value:
                            correctAnswer = value
                else:
                    correctOptDict = (Question_attemt_record['answers'].loc[quesId])
                    #print(correctOptDict)
                    correctOptDict = json.loads(correctOptDict)
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
                else:
                    incorrect_attempt = 1
                    marks_gain = int(marks_gain + negative_marking)
                    gain_mark = int(negative_marking)

                subject_id = Question_attemt_record['subject_id'].loc[quesId]
                chapter_id = Question_attemt_record['chapter_id'].loc[quesId]
                topic_id = Question_attemt_record['topic_id'].loc[quesId]
                dict = {'question_id': quesId, 'subject_id': subject_id, "chapter_id": chapter_id, 'topic_id': topic_id,
                        'gain_mark': gain_mark,
                        "attempt_correct": correct_attempt, 'attemtpt_incorrect_cnt': incorrect_attempt,
                        "attempt_cnt": 1,
                        "marks": marks, "negative_marking": negative_marking}
                dict.update(jsonable_encoder(val))
                new_answer_list.append(dict)
            #print(pd.DataFrame(new_answer_list))
            new_answer_list_df=pd.DataFrame(new_answer_list)
            if new_answer_list_df.empty:
                rank = adaptive_state['rank']
                lastbase = adaptive_state['lastbase']
                trend = adaptive_state['trend']
            else:

                new_answer_list_df['answer'] = np.where(new_answer_list_df['attempt_correct'] == 1, 'correct', 'wrong')
                answer=new_answer_list_df['answer'].to_list()
                answer=answer[0]
                rank=adaptive_state['rank']
                lastbase=adaptive_state['lastbase']
                trend=adaptive_state['trend']
                rank, trend, lastbase = getrank(rank, answer, trend, lastbase)

            questions_query = f'select question_id,topic_id,difficulty_level as difficulty_level from {quiz_bank} where topic_id={topic_id} and difficulty_level={rank} limit 1'
            question_id = await conn.execute_query_dict(questions_query)
            if question_id:
                questions_list.append(question_id[0]['question_id'])

            state_select_query = f"select id from adaptive_student_states where session_id={session_id} and topic_id={topic_id}"
            state_id = await conn.execute_query_dict(state_select_query)
            if state_id:
                statequery = f"UPDATE adaptive_student_states SET topic_id={topic_id}" \
                             f",adaptive_rank={rank},lastbase={lastbase},trend='{trend}' where session_id={session_id} and topic_id={topic_id}"
            else:
                statequery = f"INSERT INTO adaptive_student_states (chapter_id,topic_id,topic_priority,adaptive_rank,lastbase,trend,session_id)" \
                             f"VALUES ({chapter_id},{topic_id},{rank},{lastbase},'{trend}',{session_id})"
            await conn.execute_query_dict(statequery)


            # Inserting on Db student_questions_attempted for each of ques
            unattempted_questions_list = all_questions_list.copy()
            for quesDict in new_answer_list:
                question_id = int(quesDict['question_id'])
                answer = {"Answer:": quesDict['answer']}
                answer = json.dumps(answer)
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
                if attempt_correct == 1:

                    qry_update = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,time_taken,answer_swap_cnt,attempt_status,option_id,session_id) \
                                   VALUES ({exam_id},{student_id},{subject_id},{chapter_id},{topic_id},'PE',{question_id},{question_marks}, {gain_marks}, '{time_taken_sec}',{answer_swap_cnt},'Correct','{answer}','{session_id}')"
                    await conn.execute_query_dict(qry_update)
                else:
                    qry_update = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,negative_marks_cnt,time_taken,answer_swap_cnt,attempt_status,option_id,session_id) \
                                                   VALUES ({exam_id},{student_id},{subject_id},{chapter_id},{topic_id},'PE',{question_id},{question_marks}, {gain_marks},1,'{time_taken_sec}',{answer_swap_cnt},'Incorrect','{answer}','{session_id}' )"
                    await conn.execute_query_dict(qry_update)
            for unattemptQues in unattempted_questions_list:
                chapter_id = Question_attemt_record.loc[unattemptQues]['chapter_id']
                subject_id = Question_attemt_record.loc[unattemptQues]['subject_id']
                topic_id = Question_attemt_record.loc[unattemptQues]['topic_id']
                if new_answer_list:
                    question_marks = int(new_answer_list[0]['marks'])
                else:
                    question_marks=marks
                qry_insert2 = f"INSERT INTO student_questions_attempted(class_exam_id,student_id,subject_id,chapter_id,topic_id,exam_type,question_id,question_marks,gain_marks,negative_marks_cnt,time_taken,answer_swap_cnt,attempt_status,session_id) \
                                   VALUES ({exam_id},{student_id},{subject_id},{chapter_id},{topic_id},'PE',{unattemptQues},{question_marks}, 0, 0, '00:00:00',0,'Unanswered','{session_id}')"
                await conn.execute_query_dict(qry_insert2)

        if questions_list:
            total_time = exam_time_per_ques * len(questions_list)

            if len(questions_list) == 1:
                question_list_str = "(" + str(questions_list[0]) + ")"
            else:
                # print(questions_list)
                question_list_str = tuple(questions_list)

            query = f'select qb.question_id, qb.subject_id,qb.chapter_id, qb.topic_id , qb.difficulty_level, qb.question, qb.template_type, \
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

            response = {"time_allowed": l1,"session_id":session_id, "questions": l2, "success": True}
            jsonstr = json.dumps(l2, ensure_ascii=False).encode('utf8')
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
        return JSONResponse(status_code=400, content={'error': f'{e}', "success": False})