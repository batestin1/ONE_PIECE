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


censu_notuser_fruit_all = spark.sql("""SELECT _id, first_name, last_name, gender, race, birthday, age,
type_of_fruit, 
job, current_job,  contracting_company, start_date,  year_working_time,  initial_salary, current_wage,
type_of_tatoo, where_in_body, color_of_tatoo,  color_skill, color_eyes, scar, color_hair, has_disability, security_social_number,phone,
main_crime, code_crime,  tax_collected_government,  debt_with_government,rewards, 
sketch, register_data as data_of_register
FROM one WHERE type_of_fruit in ('it does not have')""").createOrReplaceTempView("censu_notuser_fruit_filtered")

censu_notuser_fruit_filtered = spark.sql("""SELECT _id, first_name, last_name, gender, race, birthday, age,
has_disability, color_hair, color_skill, scar, color_eyes, security_social_number, phone, sketch
FROM censu_notuser_fruit_filtered

""").createOrReplaceTempView("censu_notuser_fruit_payload ") 

censu_notuser_fruit_payload = spark.sql("""SELECT struct(struct(first_name, last_name, gender, race, birthday, age) as personal_characteristics,
struct(has_disability, color_hair, color_skill, scar, color_eyes) as physical_characteristics, 
struct(security_social_number, phone, sketch) as social_characteristics
) as payload FROM censu_notuser_fruit_filtered

""")

censu_user_fruit_all = spark.sql("""SELECT first_name, last_name, gender, race, birthday, age,
type_of_fruit, fruit_name, fruit_category, number_times_resurrected,
job, current_job,  contracting_company, start_date,  year_working_time,  initial_salary, current_wage,
type_of_tatoo, where_in_body, color_of_tatoo, color_eyes, color_hair, has_disability, security_social_number,phone,
main_crime, code_crime,  tax_collected_government,  debt_with_government, rewards, sketch, register_data
FROM one WHERE type_of_fruit NOT IN ('it does not have') AND number_times_resurrected < 50""").createOrReplaceTempView("censu_user_fruit_payload")

censu_user_fruit_payload = spark.sql("""SELECT
struct(struct(first_name, last_name, gender, race, birthday, age) as personal_characteristics,
struct(type_of_fruit, fruit_name, fruit_category,  number_times_resurrected) as fruit_characteristics,
struct(job, current_job,  contracting_company, start_date,  year_working_time, initial_salary, current_wage) as job_characteristics,
struct(type_of_tatoo, where_in_body, color_of_tatoo, color_eyes, color_hair, has_disability) as physical_characteristics,
struct(security_social_number,phone, sketch) as social_characteristics,
struct(main_crime, code_crime,  tax_collected_government, debt_with_government,rewards) as rewards_informations
) as payload FROM censu_user_fruit_payload
""")


###################load########################################
censu_notuser_fruit_payload.write.mode("overwrite").format("orc").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/output/onepiece/censu_notuser_fruit_payload")
censu_user_fruit_payload.write.mode("overwrite").format("orc").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/output/onepiece/censu_user_fruit_payload")
censu_notuser_fruit_payload.write.format("mongo").mode("append").option("database", "one_piece").option("collection", "not_fruit_user").save()
censu_user_fruit_payload.write.format("mongo").mode("append").option("database", "one_piece").option("collection", "fruit_user").save()



print("Data populated successfully!")