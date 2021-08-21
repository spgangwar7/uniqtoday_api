import json
import traceback
from http import HTTPStatus
from typing import List
import pandas as pd
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
        conn = Tortoise.get_connection("default")
        subjectquery = f'SELECT subject_id FROM exam_subjects where class_exam_id={exam_id}'
        subjectslist = await conn.execute_query_dict(subjectquery)
        subjectslist=[next(iter(d.values())) for d in subjectslist]
        subjectslist=",".join(str(x) for x in subjectslist)
        #print(subjectslist)
        query = f'SELECT id,sr.subject_id ,sr.chapter_id,esc.chapter_name,sum(resource_type="Video") as Videos, \
                sum(resource_type="Notes") as Notes ,sum(resource_type="Presentations") as Presentations FROM  \
                subject_resources as sr inner join exam_subject_chapters as esc on sr.chapter_id=esc.chapter_id where  \
                sr.subject_id in ({subjectslist}) group by subject_id,chapter_id'
        val = await conn.execute_query_dict(query)
        df=pd.DataFrame(val)
        #print(df)

        bookmarkquery=f'SELECT sqt.subject_id,sqt.chapter_id,esc.chapter_name, count(*) as count FROM student_question_tagged as sqt \
                        left join exam_subject_chapters as esc on sqt.chapter_id=esc.chapter_id\
                        where sqt.chapter_id IS NOT NULL and sqt.subject_id in ({subjectslist})\
                        group by sqt.subject_id,sqt.chapter_id ;'
        bookmarksval = await conn.execute_query_dict(bookmarkquery)
        bookmarksdf=pd.DataFrame(bookmarksval)
        print(bookmarksdf)
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
                    "Presentations":float(resourcedict['Presentations']),"Bookmarks":float(bookmarksrowdict['count'])})
            else:
                bookmarksrow=bookmarksdf.loc[(bookmarksdf['subject_id'] == resourcedict['subject_id']) & (bookmarksdf['chapter_id'] == resourcedict['chapter_id'])]
                if not bookmarksrow.empty:
                    bookmarksrowdict=bookmarksrow.to_dict("records")
                    bookmarksrowdict=bookmarksrowdict[0]
                    #print(bookmarksrowdict)
                    output_dict.append({"subject_id": resourcedict['subject_id'],
                    "values": [{"chapter_id": resourcedict['chapter_id'],"chapter_name": resourcedict['chapter_name'],
                    "Videos": float(resourcedict['Videos']), "Notes": float(resourcedict['Notes']),
                    "Presentations": float(resourcedict['Presentations']),"Bookmarks": float(bookmarksrowdict['count'])}]
                                        })
                    bookmarksdf=bookmarksdf.drop(bookmarksrow.index)
                else:
                    output_dict.append({"subject_id":resourcedict['subject_id'],"values":[{"chapter_id":resourcedict['chapter_id'],
                    "chapter_name":resourcedict['chapter_name'],"Videos":float(resourcedict['Videos']),"Notes":float(resourcedict['Notes']),
                    "Presentations":float(resourcedict['Presentations']),"Bookmarks":0}]})
        #Adding bookmarks whose subject id and chapter id are not present in result
        bookmarkslist=bookmarksdf.to_dict("records")
        for bookmarksdict in bookmarkslist:
            print(bookmarksdict)
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
                {"subject_id": bookmarksdict['subject_id'], "values": [{"chapter_id": bookmarksdict['chapter_id'],
                 "chapter_name": bookmarksdict['chapter_name'],"Videos": 0,
                 "Notes": 0, "Presentations": 0,
                  "Bookmarks": float(bookmarksdict['count'])}]})
        #print(bookmarksdf)

        return JSONResponse(status_code=200,content={'response': output_dict,"success":True})
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return JSONResponse(status_code=400, content={'error': f'{e}',"success":False})