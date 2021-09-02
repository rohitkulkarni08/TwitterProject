#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import pandas as pd
from pyspark.sql.functions import *
import numpy as np
from pyspark.sql.functions import col

# In[2]:


import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Cleaning-Data").getOrCreate()
print(spark)


# In[5]:


def removenull(df,x):
    df = df.withColumn(x, when(col(x) == '', None).otherwise(col(x)))
    df=df.na.drop()
    return df;


# In[8]:


def remove_duplicates(df,x):
    df=df.dropDuplicates([x])
    return df;

