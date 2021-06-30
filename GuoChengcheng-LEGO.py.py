#!/usr/bin/python
# -*- coding: utf-8 -*-
import time, sys, re, os,glob
import pandas as pd
import numpy as np
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.pyplot import plot,savefig#####for plot
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc
from pyspark.sql import DataFrame
from pyspark import sql, SparkContext, SparkConf, SparkFiles
#time measure##################################################################
time_dict= {}
tic1= int(round(time.time()* 1000))
#CONFIGURE THE CLUSTER
conf = SparkConf().setMaster("local").setAppName("LegoCounting")
print(conf.toDebugString())
conf = SparkConf() . setAll([('spark.executor.memory','64g'),('spark.executor.cores','18'),('spark.cores.max','3'),('spark.driver.memory','8g')])
conf.set('spark.dynamicAllocation.enabled','True')
conf.set('spark.dynamicAllocation.minExecutors','21')
conf.set('spark.dynamicAllocation.maxExecutors','21')
sc=SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
sqlContext =SQLContext(sc)
#READ DATASET TO DATAFRAME AND DATA CLEANING
df=sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('sortbyyear/*.csv')
dfnot01=df.filter(df.USPrice.isNotNull())
dfnot0=dfnot01.filter(dfnot01.Pieces>=25)#.show(25)
#每年共多少套how many sets per year launch###############################
df_data1=dfnot0.groupby('Year').count().orderBy('Year')#.show(25)
dfpd_data1=df_data1.toPandas()
fig =plt.figure()#new figure
ax =fig.add_subplot(111)
#print(dfpd_data1.head())
fig=dfpd_data1.plot('Year','count',kind='line')
plt.title('How many sets of LEGO per year were launched')
plt.xlabel('Year')
plt.ylabel('set')
fig.figure.savefig('Lego07.png')
#how  many pieces per set per year每年每一套 多少快##############################
df_data2=dfnot0.groupby('Year').agg({'Pieces':'mean'}).orderBy('Year')#.show(25)
df_data2=df_data2.filter(df_data2.Year>=1967)#.show(25)
dfpd_data2=df_data2.toPandas()
fig =plt.figure()#new figure
ax =fig.add_subplot(111)
#print(dfpd_data2.head())
fig=dfpd_data2.plot('Year','avg(Pieces)',kind='line')
plt.title('How many pieces per set every year')
plt.xlabel('Year')
plt.ylabel('avg(Pieces)')
fig.figure.savefig('lego08.jpg')
#how much per pieces per year每年每一快 多少钱###############################
df_data3=dfnot0.withColumn('PiecesPrice',df.USPrice/df.Pieces).select('Year','PiecesPrice').groupby('Year').agg({'PiecesPrice':'mean'}).orderBy('Year')#.show(25)
dfpd_data3=df_data3.toPandas()
fig =plt.figure()#new figure
ax =fig.add_subplot(111)
fig=dfpd_data3.plot('Year','avg(PiecesPrice)',kind='line')
plt.title('average price per piece')
plt.xlabel('Year')
plt.ylabel('avg(PiecesPrice)')
fig.figure.savefig('lego11.jpg')
#price per set每一套多少钱#############################
df_data4=dfnot01.groupby('Year').agg({'USPrice':'mean'}).orderBy('Year')#.show(25)
df_data4=df_data4.filter(df_data4.Year>=1964)
dfpd_data4=df_data4.toPandas()
fig =plt.figure()#new figure
ax =fig.add_subplot(111)
fig=dfpd_data4.plot('Year','avg(USPrice)',kind='line')
plt.title('Average price per set')
plt.xlabel('Year')
plt.ylabel('avg(USPrice)')
fig.figure.savefig('lego15.jpg')
#time measure##################################################################
tac1 = int(round(time.time() * 1000))
time_dict['Time: '] = tac1-tic1
#print(sc.getConf().getAll())
print(sc.getConf().get('spark.executor.memory'))
print(sc.getConf().get('spark.executor.cores'))
print(sc.getConf().get('spark.cores.max'))
print(sc.getConf().get('spark.driver.memory'))
print(time_dict)
sc.stop()
