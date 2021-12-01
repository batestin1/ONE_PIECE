#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: One Piece
#     Repositorio: stagin bronze
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting processing for Silver file...")

###################extrac########################################

add = spark.read.orc("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/orc/address/").createOrReplaceTempView("add")
fruit = spark.read.orc("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/orc/fruit/").createOrReplaceTempView("fruit")
job = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/parquet/job/").createOrReplaceTempView("job")
persona = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/parquet/persona/").createOrReplaceTempView("persona")
rewards = spark.read.orc("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/orc/rewards/").createOrReplaceTempView("rewards")
phy = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/parquet/physical_characteristics/").createOrReplaceTempView("phy")

######################deduplication##################################
add = spark.sql(""" SELECT l. 
* FROM (SELECT _id, region_birth, country_birth, city_birth, current_country, current_city, 
street, number, postalcode, mailer, register_data, row_number() over 
(partition by _id order by register_data desc) as row_id FROM add WHERE TRIM(_id) <> '') l WHERE row_id = 1""")

fruit = spark.sql(""" SELECT l. 
* FROM (SELECT _id, type_of_fruit, fruit_name, fruit_category, number_times_resurrected, register_data, row_number() over 
(partition by _id order by register_data desc) as row_id FROM fruit WHERE TRIM(_id) <> '') l WHERE row_id = 1""")

job = spark.sql(""" SELECT l. 
* FROM (SELECT _id, job, current_job, contracting_company, start_date, year_working_time, initial_salary, current_wage, register_data, row_number() over 
(partition by _id order by register_data desc) as row_id FROM job WHERE TRIM(_id) <> '') l WHERE row_id = 1""")

persona = spark.sql(""" SELECT l. 
* FROM (SELECT _id, first_name, last_name, gender, race, birthday, age, devil_fruit_user,has_job,
has_tatoo, has_scar, has_disability, security_social_number, phone, has_rewards, sketch, register_data, row_number() over 
(partition by _id order by register_data desc) as row_id FROM persona WHERE TRIM(_id) <> '') l WHERE row_id = 1""")

phy = spark.sql(""" SELECT l. 
* FROM (SELECT _id, color_hair, color_skill, type_of_tatoo, where_in_body, color_of_tatoo, scar, color_eyes,register_data, row_number() over 
(partition by _id order by register_data desc) as row_id FROM phy WHERE TRIM(_id) <> '') l WHERE row_id = 1""")

rewards = spark.sql(""" SELECT l. 
* FROM (SELECT _id, ssn_people, main_crime, code_crime, tax_collected_government, debt_with_government, rewards, register_data,  row_number() over 
(partition by _id order by register_data desc) as row_id FROM rewards WHERE TRIM(_id) <> '') l WHERE row_id = 1""")

######################load##################################
phy.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/parquet/physical_characteristics/")
phy.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/orc/physical_characteristics/")

add.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/parquet/address/")
add.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/orc/address/")

fruit.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/parquet/fruit/")
fruit.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/orc/fruit/")


job.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/parquet/job/")
job.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/orc/job/")

rewards.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/parquet/rewards/")
rewards.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/orc/rewards/")

persona.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/parquet/persona/")
persona.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/orc/persona/")

