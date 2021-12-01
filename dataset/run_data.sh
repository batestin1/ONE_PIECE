#! /usr/bin/env bash

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: One Piece
#     Start Bash DataSet
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
rm -r json_files
mkdir json_files
cd json_files
mkdir fruit
mkdir persona
mkdir address
mkdir rewards
mkdir job
mkdir physical_characteristics

cd ..

cd script
python main.py

echo "EXPAND YOUR MAPS"
echo "YOUR ONE PIECE POPULATION CENSUS IS BEING GENERATED!"
echo "SUCCESSFULLY GENERATED DATA! "

