import json
import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
import redis
from datetime import datetime,timedelta
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from tortoise.exceptions import  *
from tortoise import Tortoise
from fastapi.responses import JSONResponse
router = APIRouter(
    prefix='/api/subjectResources',
    tags=['Subject Resources'],
)

@router.get('/notes/{chapter_id}', description='Get Notes', status_code=201)
async def get_Notes(chapter_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select id,subject_id,chapter_id,topic_id,resource_name,resource_desc,resource_file,resource_link from subject_resources where chapter_id={chapter_id} and resource_type="Notes"'
        val = await conn.execute_query_dict(query)
        return JSONResponse(status_code=200,content={'Notes': val,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})

@router.get('/videos/{chapter_id}', description='Get Videos', status_code=201)
async def get_Videos(chapter_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select id,subject_id,chapter_id,topic_id,resource_name,resource_desc,resource_file,resource_link from subject_resources where chapter_id={chapter_id} and resource_type="Video"'
        val = await conn.execute_query_dict(query)
        return JSONResponse(status_code=200,content={'Videos': val,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})

@router.get('/presentations/{chapter_id}', description='Get Presentation', status_code=201)
async def get_Videos(chapter_id:int=0):
    try:
        conn = Tortoise.get_connection("default")
        query = f'select id,subject_id,chapter_id,topic_id,resource_name,resource_desc,resource_file,resource_link from subject_resources where chapter_id={chapter_id} and resource_type="Presentations"'
        val = await conn.execute_query_dict(query)
        return JSONResponse(status_code=200,content={'Presentations': val,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get("/bookmarks/{student_id}/{exam_id}/{chapter_id}")
async def getBookmarks(student_id:int=0,exam_id:int=0,chapter_id:int=0):
    conn=Tortoise.get_connection('default')
    query=f"SELECT esc.chapter_name,COUNT(DISTINCT(sqt.question_id)) AS bookmark_count " \
          f"FROM exam_subject_chapters AS esc LEFT JOIN student_question_tagged sqt ON " \
          f"esc.chapter_id=sqt.chapter_id WHERE sqt.chapter_id={chapter_id} AND sqt.exam_id={exam_id} AND sqt.student_id={student_id} limit 1"
    val = await conn.execute_query_dict(query)
    if not val:
        return JSONResponse(status_code=400, content={"response": "No bookmark questions for user", "success": False})
    chapter_name = val[0]['chapter_name']
    bookmark_count = val[0]['bookmark_count']

    r = redis.Redis()
    question_bank_name = ''
    if r.exists(str(exam_id) + "_examid"):
        exam_cache = json.loads(r.get(str(exam_id) + "_examid"))
        if "question_bank_name" in exam_cache:
            question_bank_name = exam_cache['question_bank_name']
        else:
            query = f"select question_bank_name from question_bank_tables where exam_id={exam_id} limit 1"
            res = await conn.execute_query_dict(query)
            question_bank_name = res[0]['question_bank_name']
            exam_cache = {
                'question_bank_name': question_bank_name
            }
            r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))
    else:
        query = f"select question_bank_name from question_bank_tables where exam_id={exam_id} limit 1"
        res = await conn.execute_query_dict(query)
        question_bank_name = res[0]['question_bank_name']
        exam_cache = {
            'question_bank_name': question_bank_name
        }
        r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))

    query = f'select distinct(ques.question_id),ques.question,ques.tags ' \
            f'from {question_bank_name} as ques left join question_bank_passage_inst as passage on passage.id=ques.passage_inst_id ' \
            f'left join student_question_tagged as sqt on sqt.question_id=ques.question_id where sqt.student_id={student_id} and sqt.chapter_id={chapter_id} and sqt.exam_id={exam_id} group by sqt.question_id'
    book_mark_questions = await conn.execute_query_dict(query)


    resp = {
        "chapter_id": chapter_id,
        "chapter_name": chapter_name,
        "total_bookmarks": bookmark_count,
        "bookmark_questions": book_mark_questions
    }
    return JSONResponse(status_code=200, content=resp)

def retro_dictify(frame):
    d = {}
    for row in frame.values:
        here = d
        for elem in row[:-2]:
            if elem not in here:
                here[elem] = {}
            here = here[elem]
        here[row[-2]] = row[-1]
    return d

@router.get('/chapterWiseSummary/{exam_id}/{student_id}', description='Get Notes', status_code=201)
async def get_chapter_wise_summary(exam_id:int=0,student_id:int=0):
    try:
        conn = Tortoise.get_connection('default')
        subjectquery = f'SELECT distinct(subject_id) FROM exam_subjects where class_exam_id={exam_id}'
        subjects_list = await conn.execute_query_dict(subjectquery)
        subjects_df = pd.DataFrame(subjects_list)
        subjectslist = subjects_df['subject_id'].tolist()
        print(subjectslist)
        query = f'SELECT sr.id,sr.subject_id,subjects.subject_name,sr.chapter_id,sr.resource_name,sr.resource_desc,sr.resource_type,' \
                f'sr.resource_file,sr.resource_link,esc.chapter_name,sum(sr.resource_type="Video") as Videos,' \
                f'sum(sr.resource_type="Notes") as Notes ,sum(sr.resource_type="Presentations") as Presentations FROM ' \
                f'subject_resources as sr inner join exam_subject_chapters as esc on sr.chapter_id=esc.chapter_id join subjects on sr.subject_id=subjects.id where ' \
                f'sr.subject_id in {tuple(subjectslist)} and sr.class_id={exam_id} group by subject_id,chapter_id'
        val = await conn.execute_query_dict(query)


        bookmarkquery=f'SELECT sqt.subject_id,subjects.subject_name,sqt.chapter_id,esc.chapter_name, count(*) as count FROM student_question_tagged as sqt \
                        left join exam_subject_chapters as esc on sqt.chapter_id=esc.chapter_id join subjects on sqt.subject_id=subjects.id\
                        where sqt.chapter_id IS NOT NULL and sqt.subject_id in {tuple(subjectslist)}  and student_id={student_id}\
                        group by sqt.subject_id,sqt.chapter_id ;'
        bookmarksval = await conn.execute_query_dict(bookmarkquery)
        bookmarksdf=pd.DataFrame(bookmarksval)
        #print(bookmarksdf)
        output_dict=[]
        for resourcedict in val:
            if any(d["subject_id"] == resourcedict['subject_id'] for d in output_dict):
                #print("value already exits"+str(resourcedict['subject_id']))
                subject_id=resourcedict['subject_id']
                chapter_id=resourcedict['chapter_id']
                bookmarksrow=bookmarksdf.loc[(bookmarksdf['subject_id'] == resourcedict['subject_id']) & (bookmarksdf['chapter_id'] == resourcedict['chapter_id'])]
                #print("subject_id: "+str(subject_id)+" chapter_id: "+str(chapter_id))
                if not bookmarksrow.empty:
                    bookmarksrowdict=bookmarksrow.to_dict("records")
                    bookmarksrowdict=bookmarksrowdict[0]
                    subject_dict = next(item for item in output_dict if item["subject_id"] == resourcedict['subject_id'])
                    values = subject_dict.get("values")
                    values=values.append({"chapter_id":resourcedict['chapter_id'],"chapter_name":resourcedict['chapter_name'],
                    "Videos":float(resourcedict['Videos']),"Notes":float(resourcedict['Notes']),
                    "Presentations":float(resourcedict['Presentations']),"Bookmarks":0})
                    bookmarksdf=bookmarksdf.drop(bookmarksrow.index)
                else:
                    subject_dict=next(item for item in output_dict if item["subject_id"] == resourcedict['subject_id'])
                    values=subject_dict.get("values")
                    values=values.append({"chapter_id":resourcedict['chapter_id'],"chapter_name":resourcedict['chapter_name'],
                    "Videos":float(resourcedict['Videos']),"Notes":float(resourcedict['Notes']),
                    "Presentations":float(resourcedict['Presentations']),"Bookmarks":float(bookmarksrowdict['count'])
                                          })
            else:
                if not bookmarksdf.empty:
                    bookmarksrow=bookmarksdf.loc[(bookmarksdf['subject_id'] == resourcedict['subject_id']) & (bookmarksdf['chapter_id'] == resourcedict['chapter_id'])]
                    if not bookmarksrow.empty:
                        bookmarksrowdict=bookmarksrow.to_dict("records")
                        bookmarksrowdict=bookmarksrowdict[0]
                        #print(bookmarksrowdict)
                        output_dict.append({"subject_id": resourcedict['subject_id'],"subject_name": resourcedict['subject_name'],
                        "values": [{"chapter_id": resourcedict['chapter_id'],"chapter_name": resourcedict['chapter_name'],
                        "Videos": float(resourcedict['Videos']), "Notes": float(resourcedict['Notes']),
                        "Presentations": float(resourcedict['Presentations']),"Bookmarks": float(bookmarksrowdict['count'])

                                    }]
                                            })
                        bookmarksdf=bookmarksdf.drop(bookmarksrow.index)
                    else:
                        output_dict.append(
                            {"subject_id": resourcedict['subject_id'], "subject_name": resourcedict['subject_name'],
                             "values": [{"chapter_id": resourcedict['chapter_id'],
                                         "chapter_name": resourcedict['chapter_name'],
                                         "Videos": float(resourcedict['Videos']), "Notes": float(resourcedict['Notes']),
                                         "Presentations": float(resourcedict['Presentations']), "Bookmarks": 0}]})
                else:
                    output_dict.append({"subject_id":resourcedict['subject_id'] ,"subject_name": resourcedict['subject_name'],"values":[{"chapter_id":resourcedict['chapter_id'],
                    "chapter_name":resourcedict['chapter_name'],"Videos":float(resourcedict['Videos']),"Notes":float(resourcedict['Notes']),
                    "Presentations":float(resourcedict['Presentations']),"Bookmarks":0}]})
        #Adding bookmarks whose subject id and chapter id are not present in result
        bookmarkslist=bookmarksdf.to_dict("records")
        for bookmarksdict in bookmarkslist:
            #print(bookmarksdict)
            if any(d["subject_id"] == bookmarksdict['subject_id'] for d in output_dict):
                subject_dict = next(item for item in output_dict if item["subject_id"] == bookmarksdict['subject_id'])
                values = subject_dict.get("values")
                values = values.append(
                {"chapter_id": bookmarksdict['chapter_id'],
                 "chapter_name": bookmarksdict['chapter_name'],"Videos": 0,
                 "Notes": 0, "Presentations": 0,
                  "Bookmarks": float(bookmarksdict['count'])})
            else:
                output_dict.append(
                {"subject_id": bookmarksdict['subject_id'],"subject_name": bookmarksdict['subject_name'], "values": [{"chapter_id": bookmarksdict['chapter_id'],
                 "chapter_name": bookmarksdict['chapter_name'],"Videos": 0,
                 "Notes": 0, "Presentations": 0,
                  "Bookmarks": float(bookmarksdict['count'])}]})
        #print(bookmarksdf)

        return JSONResponse(status_code=200,content={'response': output_dict,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})


@router.get('/subject-wise-resources/{student_id}/{exam_id}/{subject_id}')
async def subjectWiseResources(exam_id:int=0,student_id:int=0,subject_id:int=0):
    conn=Tortoise.get_connection('default')
    subject_query=f'SELECT subject_name FROM subjects where id={subject_id};'
    subject_dict=await conn.execute_query_dict(subject_query)
    subject_name=subject_dict[0]['subject_name']
    query = f"select count(distinct(question_id)) as bookmark_count from student_question_tagged as sqt  where sqt.subject_id={subject_id} and sqt.exam_id={exam_id} and sqt.student_id={student_id} limit 1"
    val = await conn.execute_query_dict(query)
    if not val:
        bookmark_count = 0
    else:
        bookmark_count=val[0]['bookmark_count']
    r=redis.Redis()
    question_bank_name=''
    if r.exists(str(exam_id)+"_examid"):
        exam_cache=json.loads(r.get(str(exam_id)+"_examid"))
        if "question_bank_name" in exam_cache:
            question_bank_name=exam_cache['question_bank_name']
        else:
            query=f"select question_bank_name from question_bank_tables where exam_id={exam_id} limit 1"
            res=await conn.execute_query_dict(query)
            question_bank_name=res[0]['question_bank_name']
            exam_cache={
                'question_bank_name':question_bank_name
            }
            r.setex(str(exam_id)+"_examid",timedelta(days=1),json.dumps(exam_cache))
    else:
        query = f"select question_bank_name from question_bank_tables where exam_id={exam_id} limit 1"
        res = await conn.execute_query_dict(query)
        question_bank_name = res[0]['question_bank_name']
        exam_cache = {
            'question_bank_name': question_bank_name
        }
        r.setex(str(exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))
    query = f'select distinct(ques.question_id),ques.question,ques.tags ' \
            f'from {question_bank_name} as ques left join question_bank_passage_inst as passage on passage.id=ques.passage_inst_id ' \
            f'left join student_question_tagged as sqt on sqt.question_id=ques.question_id where sqt.student_id={student_id} and sqt.subject_id={subject_id} and sqt.exam_id={exam_id} group by sqt.question_id'
    book_mark_questions=await conn.execute_query_dict(query)

    query=f"SELECT sr.chapter_id,esc.chapter_name,sr.topic_id,sr.resource_name,sr.resource_desc,sr.resource_type," \
          f" sr.resource_file,sr.resource_link FROM subject_resources as sr left join exam_subject_chapters as esc on sr.chapter_id=esc.chapter_id WHERE sr.class_id={exam_id} AND sr.subject_id={subject_id}"
    res=await conn.execute_query_dict(query)
    notes_count = 0
    videos_count = 0
    presentation_count = 0
    notes_list = []
    presentation_list = []
    videos_list = []
    if  res:
        for x in res:
            if x['resource_type']=="Notes":
                notes_count+=1
                x.pop('resource_type')
                notes_list.append(x)
            elif x['resource_type']=="Video":
                videos_count+=1
                x.pop('resource_type')
                videos_list.append(x)
            elif x['resource_type']=="Presentations":
                presentation_count+=1
                x.pop('resource_type')
                presentation_list.append(x)

    resp={
        "subject_id":subject_id,
        "subject_name":subject_name,
        "total_notes":notes_count,
        "total_videos":videos_count,
        "total_bookmarks":bookmark_count,
        "total_presentations":presentation_count,
        "notes":notes_list,
        "videos":videos_list,
        "presentation":presentation_list,
        "bookmark_questions":book_mark_questions
    }
    return JSONResponse(status_code=200,content=resp)

@router.get('/overall-resources/{student_id}')
async def allResources(student_id:int=0):
    try:
        class_exam_id = ""
        r = redis.Redis()
        conn = Tortoise.get_connection("default")
        subject_cache = {}
        subject_resources=[]
        if r.exists(str(student_id) + "_sid"):
            student_cache = json.loads(r.get(str(student_id) + "_sid"))
            if "exam_id" in student_cache:
                class_exam_id = student_cache['exam_id']
            else:
                query = f'SELECT grade_id FROM student_users where id={student_id} limit 1;'  # fetch exam_id by user_id
                class_exam_id = await conn.execute_query_dict(query)
                if len(class_exam_id) == 0:
                    resp = {
                        "message": "No exam Found for this user",
                        "success": False
                    }
                    return resp, 400
                class_exam_id = int(class_exam_id[0]['grade_id'])

                student_cache['exam_id'] = class_exam_id
                r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))
        else:
            query = f'SELECT grade_id FROM student_users where id={student_id} limit 1;'  # fetch exam_id by user_id
            class_exam_id = await conn.execute_query_dict(query)
            if len(class_exam_id) == 0:
                resp = {
                    "message": "No exam Found for this user",
                    "success": False
                }
                return resp, 400
            class_exam_id = int(class_exam_id[0]['grade_id'])
            student_cache = {'exam_id': class_exam_id}
            r.setex(str(student_id) + "_sid", timedelta(days=1), json.dumps(student_cache))

        exam_cache = {}
        subjects = []
        if r.exists(str(class_exam_id) + "_examid"):
            exam_cache = json.loads(r.get(str(class_exam_id) + "_examid"))
            if "subjects" in exam_cache:
                subjects = exam_cache['subjects']
            else:
                query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={class_exam_id} group by exam_subjects.subject_id'
                subjects = await conn.execute_query_dict(query)
                exam_cache['subjects'] = subjects
                r.setex(str(class_exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))
        else:
            query = f'select subjects.id,subjects.subject_name from subjects join exam_subjects on  exam_subjects.subject_id=subjects.id where  exam_subjects.class_exam_id={class_exam_id} group by exam_subjects.subject_id'
            subjects = await conn.execute_query_dict(query)
            if len(subjects) == 0:
                return JSONResponse(status_code=400, content={"msg": f"no subjects with the given exam_id : {class_exam_id}",
                                                              "success": False})
            exam_cache['subjects'] = subjects
            r.setex(str(class_exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))

        question_bank_name = ''
        if r.exists(str(class_exam_id) + "_examid"):
            exam_cache = json.loads(r.get(str(class_exam_id) + "_examid"))
            if "question_bank_name" in exam_cache:
                question_bank_name = exam_cache['question_bank_name']
            else:
                query = f"select question_bank_name from question_bank_tables where exam_id={class_exam_id} limit 1"
                res = await conn.execute_query_dict(query)
                question_bank_name = res[0]['question_bank_name']
                exam_cache = {
                    'question_bank_name': question_bank_name
                }
                r.setex(str(class_exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))
        else:
            query = f"select question_bank_name from question_bank_tables where exam_id={class_exam_id} limit 1"
            res = await conn.execute_query_dict(query)
            question_bank_name = res[0]['question_bank_name']
            exam_cache = {
                'question_bank_name': question_bank_name
            }
            r.setex(str(class_exam_id) + "_examid", timedelta(days=1), json.dumps(exam_cache))

        for subject in subjects:
            chapter_array=[]
            subject_id=subject['id']
            query = f"select s.subject_name,count(distinct(question_id)) as bookmark_count from subjects as s left join student_question_tagged as sqt on s.id=sqt.subject_id where sqt.subject_id={subject_id} and sqt.exam_id={class_exam_id} and sqt.student_id={student_id} limit 1"
            val = await conn.execute_query_dict(query)
            if not val:
                subject_name = subject['subject_name']
                bookmark_count=0
            else:
                subject_name=subject['subject_name']
                bookmark_count=val[0]['bookmark_count']

            query = f'select distinct(ques.question_id),ques.question,ques.tags ' \
                    f'from {question_bank_name} as ques left join question_bank_passage_inst as passage on passage.id=ques.passage_inst_id ' \
                    f'left join student_question_tagged as sqt on sqt.question_id=ques.question_id where sqt.student_id={student_id} and sqt.subject_id={subject_id} and sqt.exam_id={class_exam_id} group by sqt.question_id'
            book_mark_questions=await conn.execute_query_dict(query)

            query=f"SELECT sr.chapter_id,esc.chapter_name,sr.topic_id,sr.resource_name,sr.resource_desc,sr.resource_type," \
                  f" sr.resource_file,sr.resource_link FROM subject_resources as sr left join exam_subject_chapters as esc on sr.chapter_id=esc.chapter_id WHERE sr.class_id={class_exam_id} AND sr.subject_id={subject_id}"
            res=await conn.execute_query_dict(query)
            if not res:
                notes_count = 0
                videos_count = 0
                presentation_count = 0
                notes_list = []
                presentation_list = []
                videos_list = []
            else:
                notes_count=0
                videos_count=0
                presentation_count=0
                notes_list=[]
                presentation_list=[]
                videos_list=[]
                for x in res:
                    if x['resource_type']=="Notes":
                        notes_count=notes_count+1
                        x.pop('resource_type')
                        notes_list.append(x)
                    elif x['resource_type']=="Video":
                        videos_count=videos_count+1
                        x.pop('resource_type')
                        videos_list.append(x)
                    elif x['resource_type']=="Presentations":
                        presentation_count=presentation_count+1
                        x.pop('resource_type')
                        presentation_list.append(x)
            # chapter wise code
            chapter_query = f"select chapter_id,chapter_name from exam_subject_chapters where subject_id={subject_id} and class_exam_id={class_exam_id} "
            chapters = await conn.execute_query_dict(chapter_query)
            for chapter in chapters:
                chapter_id=chapter['chapter_id']
                chapter_name=chapter['chapter_name']
                query = f"SELECT esc.chapter_name,COUNT(DISTINCT(sqt.question_id)) AS bookmark_count " \
                        f"FROM exam_subject_chapters AS esc LEFT JOIN student_question_tagged sqt ON " \
                        f"esc.chapter_id=sqt.chapter_id WHERE sqt.chapter_id={chapter_id} AND sqt.exam_id={class_exam_id} AND sqt.student_id={student_id} limit 1"
                val = await conn.execute_query_dict(query)
                if not val:
                    bookmark_count=0
                else:
                    bookmark_count = val[0]['bookmark_count']

                query = f'select distinct(ques.question_id),ques.question,ques.tags ' \
                        f'from {question_bank_name} as ques left join question_bank_passage_inst as passage on passage.id=ques.passage_inst_id ' \
                        f'left join student_question_tagged as sqt on sqt.question_id=ques.question_id where sqt.student_id={student_id} and sqt.chapter_id={chapter_id} and sqt.exam_id={class_exam_id} group by sqt.question_id'
                book_mark_questions = await conn.execute_query_dict(query)

                query = f"SELECT sr.chapter_id,esc.chapter_name,sr.topic_id,sr.resource_name,sr.resource_desc,sr.resource_type," \
                        f" sr.resource_file,sr.resource_link FROM subject_resources as sr left join exam_subject_chapters as esc on sr.chapter_id=esc.chapter_id WHERE sr.class_id={class_exam_id} AND sr.subject_id={subject_id} AND sr.chapter_id={chapter_id}"
                res = await conn.execute_query_dict(query)
                if res:
                    notes_count = 0
                    videos_count = 0
                    presentation_count = 0
                    notes_list = []
                    presentation_list = []
                    videos_list = []
                    for x in res:
                        if x['resource_type'] == "Notes":
                            notes_count += 1
                            x.pop('resource_type')
                            notes_list.append(x)
                        elif x['resource_type'] == "Video":
                            videos_count += 1
                            x.pop('resource_type')
                            videos_list.append(x)
                        elif x['resource_type'] == "Presentations":
                            presentation_count += 1
                            x.pop('resource_type')
                            presentation_list.append(x)

                    chapter_dict = {
                        "chapter_id": chapter_id,
                        "chapter_name": chapter_name,
                        "total_notes": notes_count,
                        "total_videos": videos_count,
                        "total_bookmarks": bookmark_count,
                        "total_presentations": presentation_count,
                        "notes": notes_list,
                        "videos": videos_list,
                        "presentation": presentation_list,
                        "bookmark_questions": book_mark_questions
                    }
                    chapter_array.append(chapter_dict)


            subject_dict={
                "subject_id":subject_id,
                "subject_name":subject_name,
                "total_notes":notes_count,
                "total_videos":videos_count,
                "total_bookmarks":bookmark_count,
                "total_presentations":presentation_count,
                "notes":notes_list,
                "videos":videos_list,
                "presentation":presentation_list,
                "bookmark_questions":book_mark_questions,
                "chapter_resources":chapter_array
            }
            subject_resources.append(subject_dict)

        resp={"subject_resources":subject_resources}
        return JSONResponse(status_code=200,content=resp)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={"message":"Some error occured ","success":False})