#! /usr/bin/env bash

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: One Piece
#     Start Bash 
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
cd dataset
sh run_data.sh
cd ..


echo "STARTING PROCESSING DATE"
cd scripts
python querys_bronze.py
echo "STARTING DATA NORMALIZATION"
python querys_silver.py
echo "FINISHING DATA NORMALIZATION"
python querys_gold.py
echo "LOADING IN MONGO"
python querys_final.py
cd ..
cd dataset
rm -r json_files
cd ..

#git
git add .
git commit -m "v2 - 20211202"
git push origin one