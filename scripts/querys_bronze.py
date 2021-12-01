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

print("Starting processing for Bronze file...")

###################address########################################

add = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/dataset/json_files/address/")
add.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/parquet/address/")
add.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/orc/address/")

###################fruit########################################
fruit = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/dataset/json_files/fruit/")
fruit.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/parquet/fruit/")
fruit.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/orc/fruit/")

###################job########################################
job = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/dataset/json_files/job/")
job.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/parquet/job/")
job.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/orc/job/")

###################persona########################################
persona = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/dataset/json_files/persona/")
persona.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/parquet/persona/")
persona.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/orc/persona/")

###################rewards########################################
rewards = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/dataset/json_files/rewards/")
rewards.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/parquet/rewards/")
rewards.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/orc/rewards/")

###################physical_characteristics########################################
phy = spark.read.option("multiline", "True").json("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/dataset/json_files/physical_characteristics/")
phy.write.mode("overwrite").format("parquet").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/parquet/physical_characteristics/")
phy.write.mode("overwrite").format("orc").partitionBy("register_data").save("C:/Users/Bates/Documents/Repositorios/NOSQL/one_piece/stagin/bronze/orc/physical_characteristics/")

