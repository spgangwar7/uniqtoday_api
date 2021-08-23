import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db.sparkSession import spark_session
from routers.users import router as users
from routers.exams import router as exams
from routers.student_save_result import router as student_save_result
from routers.postExamAnalytics import router as postExamAnalytics
from routers.subscriptions import router as subscriptions
from routers.registration import router as registerations
from routers.question_selection import router as question_selections
from routers.bookmarkQuestion import router as bookmarkQuestions
from routers.testSeries import router as testSeries
from tortoise.contrib.fastapi import register_tortoise
from routers.questionReview import router as questionsReviews
from routers.profilling_input import router as profilling_input
from routers.profilingTest import router as profilingTest
from routers.scholarshipTest import router as scholarshipTest
from routers.referrals import router as referrals
from routers.todayFeeling import router as todayfeelings
from routers.stageAtSignUp import router as stage_at_signup
from routers.resource import router as resources
from routers.preference import router as preference
from routers.studentPlanner import router as studentPlanner
from routers.previousYearQuestionPapers import router as previousYearQuestionPapers
from routers.leadershipBoard import router as leadershipBoards
from routers.dashboard import router as studentDashboard
from routers.subjectResources import router as subjectResources
from routers.analytics import router as analytics
from routers.payment import router as payments
from routers.adaptiveQuestions import router as adaptiveQuestions
from routers.adaptiveQuestionsOld import router as adaptiveQuestionsOld
from routers.weakTopicPrediction import router as weakTopicPredictions
from routers.goalSetting import router as goalSetting
from routers.predictStudentsEffort import router as predictStudentsEffort
from routers.studentSelectionInCe import router as studentSelectionInCe
from routers.liveExams import router as liveExams

import db.models
from os import path

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
class UNIQ_Live:
    DB_HOST="thomson-digital-db.cluster-ch6wrof78zwh.ap-south-1.rds.amazonaws.com"
    DB_USER="admin"
    DB_PASS="2xR3LauJES"
    DB_NAME="learntoday"
    DB_URL="mysql://admin:2xR3LauJES@thomson-digital-db.cluster-ch6wrof78zwh.ap-south-1.rds.amazonaws.com:3306/learntoday"

class UNIQ_UAT:
    DB_HOST="database-2.c0jbkrha6hgp.us-west-2.rds.amazonaws.com"
    DB_USER="admin"
    DB_PASS="5DBYs1ou3ACxlRjBUmfn"
    DB_NAME="learntoday_uat"
    DB_URL="mysql://admin:5DBYs1ou3ACxlRjBUmfn@database-2.c0jbkrha6hgp.us-west-2.rds.amazonaws.com:3306/learntoday_uat"

db_cofig=UNIQ_UAT()
db_url=db_cofig.DB_URL
register_tortoise(
    app,
    db_url=db_url,
    modules={"models": ["db.models"]},
    #add_exception_handlers=True,
)
app.include_router(users)
app.include_router(subscriptions)
app.include_router(payments)
app.include_router(bookmarkQuestions)
app.include_router(exams)
app.include_router(student_save_result)
app.include_router(postExamAnalytics)
app.include_router(question_selections)
app.include_router(registerations)
app.include_router(testSeries)
app.include_router(questionsReviews)
app.include_router(profilingTest)
app.include_router(scholarshipTest)
app.include_router(referrals)
app.include_router(todayfeelings)
app.include_router(stage_at_signup)
app.include_router(resources)
app.include_router(preference)
app.include_router(studentPlanner)
app.include_router(previousYearQuestionPapers)
app.include_router(leadershipBoards)
app.include_router(studentDashboard)
app.include_router(subjectResources)
app.include_router(analytics)
app.include_router(liveExams)
app.include_router(adaptiveQuestions)
app.include_router(adaptiveQuestionsOld)
app.include_router(weakTopicPredictions)
app.include_router(goalSetting)
app.include_router(predictStudentsEffort)
app.include_router(studentSelectionInCe)



@app.get('/')
async def welcome():
    return {"message":"Fastapi Running"}

if __name__=="__main__":
    import sys
    sys.path.append(path.join(path.dirname(__file__), '..'))
    uvicorn.run("main:app",host="0.0.0.0",port=8080,debug=False)