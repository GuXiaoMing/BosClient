#!/bin/bash

if [ ! -d output ]
then
    mkdir output
fi

rm -rf ./output/*

cp -r ./conf ./output
cp -r ./dao ./output
cp -r ./bll ./output
cp -r ./entry ./output

find output -name '.svn' -exec rm -rf {} \;
find output -name '*.pyc' -exec rm -rf {} \;

mkdir ./output/log
mkdir ./output/data
