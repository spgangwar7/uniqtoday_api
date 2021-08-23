from pyspark.sql import SparkSession



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

db_cofig = UNIQ_Live()
db_url = db_cofig.DB_URL
db_host=db_cofig.DB_HOST
db_user=db_cofig.DB_USER
db_pass=db_cofig.DB_PASS
db_name=db_cofig.DB_NAME
mysqlJarPath = "/jars/mysql-connector-java-8.0.23.jar"
driver = "com.mysql.jdbc.Driver"

def spark_session():

    spark = SparkSession.builder.config("spark.jars", mysqlJarPath) \
    .master("local").appName("UNIQ_PySpark_MySQl").getOrCreate()
    return spark

def get_spark_dataframe(table_name:str):
    spark=spark_session()
    spark_df = spark.read.format("jdbc").option("url", db_url) \
        .option("driver", driver).option("dbtable", table_name) \
        .option("user", db_user).option("password", db_pass).load()
    return spark_df