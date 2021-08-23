import MySQLdb
from tortoise import Tortoise
from tortoise.models import Model
from tortoise.contrib.fastapi import register_tortoise

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

def db_connection():
    db_cofig = UNIQ_Live()
    db_url = db_cofig.DB_URL
    db_host=db_cofig.DB_HOST
    db_user=db_cofig.DB_USER
    db_pass=db_cofig.DB_PASS
    db_name=db_cofig.DB_NAME
    connection = MySQLdb.connect(host=db_host,
    user=db_user,
    password=db_pass,charset='utf8',port=3306) # create the connection
    cursor = connection.cursor() # get the cursor
    cursor.execute('use '+db_name) # select the DB
    return connection,cursor

