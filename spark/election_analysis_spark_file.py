import pandas as pd
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("spark_session").getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better

df_dep = spark.read.csv("p2022-res-dep-t1.csv", header=True, inferSchema=True)
df_reg = spark.read.csv("p2022-res-reg-t1.csv", header=True, inferSchema=True)

df_dep.limit(5)
df_reg.limit(5)

df_dep.printSchema()
df_reg.printSchema()


#cleaning the dataframe by replacing "." by "_" to be able to manipulate
# the dataframe
df_r = df_reg.toDF(*[x.replace(".","_") for x in df_reg.columns])
df_p = df_dep.toDF(*[x.replace(".","_") for x in df_dep.columns])

# verifying that the changes were applied
df_r.printSchema()
df_p.printSchema()



# Find Count of missing values in the regional votes dataframes
from pyspark.sql.functions import col,isnan,when,count
df2 = df_r.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in df_r.columns])
df2.show()

# Find Count of missing values in the departemental votes dataframes
from pyspark.sql.functions import col,isnan,when,count
df3 = df_p.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in df_p.columns])
df3.show()


# We observe that the are no missing values and 
# the dataframes are well defined



     