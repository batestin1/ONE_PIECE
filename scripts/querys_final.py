#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: One Piece
#     Repositorio: output/MONGO
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as sfunc
import pyspark.sql.types as stypes
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)




#spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

spark: SparkSession = SparkSession.builder.appName("MyApp").config("spark.mongodb.input.uri", "mongodb://localhost:27017").config("spark.mongodb.output.uri", "mongodb://localhost:27017").config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").master("local").getOrCreate()


print("Starting processing for output file...")

###################extrac########################################

one = spark.read.orc("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/gold/orc/one/").createOrReplaceTempView("one")

###################transform########################################


one = spark.sql("""SELECT _id, first_name, last_name, gender, race, birthday, age,
struct(type_of_fruit, fruit_name,fruit_category,  number_times_resurrected ) as akuma_no_mi,
struct( job, current_job,  contracting_company, start_date,  year_working_time,  initial_salary, current_wage  ) as job,
type_of_tatoo, where_in_body, color_of_tatoo, color_eyes, color_hair, has_disability, security_social_number,phone,
struct( main_crime, code_crime,  tax_collected_government,  debt_with_government, rewards ) as rewards,
sketch, register_data as data_of_register
FROM one""")

###################load########################################

one.write.format("mongo").mode("append").option("database", "one_piece").option("collection", "censu").save()

one.write.mode("overwrite").format("orc").partitionBy("data_of_register").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/output/onepiece/")



print("Data populated successfully!")