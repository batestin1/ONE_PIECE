#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: One Piece
#     Repositorio: stagin silver
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting processing for gold file...")

###################extrac########################################

add = spark.read.orc("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/orc/address/").createOrReplaceTempView("add")
fruit = spark.read.orc("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/orc/fruit/").createOrReplaceTempView("fruit")
job = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/parquet/job/").createOrReplaceTempView("job")
persona = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/parquet/persona/").createOrReplaceTempView("persona")
rewards = spark.read.orc("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/orc/rewards/").createOrReplaceTempView("rewards")
phy = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/silver/parquet/physical_characteristics/").createOrReplaceTempView("phy")

###################transform########################################



one = spark.sql("""
SELECT PE._id, PE.first_name, PE.last_name, PE.gender, PE.race, PE.birthday, PE.age, 
FR.type_of_fruit, FR.fruit_name, FR.fruit_category, FR.number_times_resurrected,
JO.job, JO.current_job, JO.contracting_company, JO.start_date, JO.year_working_time, JO.initial_salary, JO.current_wage,
PH.type_of_tatoo, PH.where_in_body, PH.color_of_tatoo, PH.scar, PH.color_eyes, PH.color_hair, PH.color_skill,
PE.has_disability, PE.security_social_number, PE.phone,
RE.main_crime, RE.code_crime, RE.tax_collected_government, RE.debt_with_government, RE.rewards,
PE.sketch, PE.register_data
FROM persona PE INNER JOIN fruit FR ON PE._id = FR._id
INNER JOIN job JO ON PE._id = JO._id
INNER JOIN phy PH on PE._id = PH._id
INNER JOIN rewards RE on PE._id = RE._id """)

###################load########################################
one.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/gold/orc/one/")

