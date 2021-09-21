import traceback
import pickle
import pandas as pd
import numpy as np
import traceback
from builtins import print
from datetime import datetime, time, date
import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import APIRouter


router = APIRouter(
    prefix='/api',
    tags=['Assessment Questions Predictor'],
)

path_pickle='pickle/assessment_questions/'
@router.get('/assessment-questions-predictor/{exam_id}', description='get weak topics for assessment questions')
async def PredictWeakTopics(exam_id:int=0):
    try:
        rules=pd.DataFrame()
        if exam_id == 1:
            print("Getting topics for JEE")
            pkl_filename = path_pickle + 'jeeTopics.pkl'
            file = open(pkl_filename, 'rb')
            rules = pickle.load(file)
            file.close()
            #print(rules)
            output = rules.query('antecedents in (3137,3717,3140)')
            output = output.sort_values(['confidence'], ascending=[False])
            topic_id_list = output['consequents'].unique().tolist()
            print(topic_id_list)
            return {"topic_id_list":topic_id_list}
        if exam_id == 2:
            print("Getting topics for NEET")
            pkl_filename = path_pickle + 'neetTopics.pkl'
            file = open(pkl_filename, 'rb')
            rules = pickle.load(file)
            file.close()
            output = rules.query('antecedents in (3816,4429)')
            output = output.sort_values(['confidence'], ascending=[False])
            topic_id_list = output['consequents'].unique().tolist()
            print(topic_id_list)
            return {"topic_id_list":topic_id_list}
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
