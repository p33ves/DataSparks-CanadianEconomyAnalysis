import sys, datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import to_date, round, lit, avg, year
from pyspark.sql.functions import *
from pyspark.sql import Row
import pandas as pd

IMF_schema = types.StructType([
    types.StructField('Subject Descriptor', types.StringType()),
    types.StructField('Units', types.StringType()),
    types.StructField('Scale', types.StringType()),
    types.StructField('Country/Series-specific Notes', types.StringType()),
    types.StructField('2000',types.DoubleType()),
    types.StructField('2001',types.DoubleType()),
    types.StructField('2002',types.DoubleType()),
    types.StructField('2003',types.DoubleType()),
    types.StructField('2004',types.DoubleType()),
    types.StructField('2005',types.DoubleType()),
    types.StructField('2006',types.DoubleType()),
    types.StructField('2007',types.DoubleType()),
    types.StructField('2008',types.DoubleType()),
    types.StructField('2009',types.DoubleType()),
    types.StructField('2010',types.DoubleType()),
    types.StructField('2011',types.DoubleType()),
    types.StructField('2012',types.DoubleType()),
    types.StructField('2013',types.DoubleType()),
    types.StructField('2014',types.DoubleType()),
    types.StructField('2015',types.DoubleType()),
    types.StructField('2016',types.DoubleType()),
    types.StructField('2017',types.DoubleType()),
    types.StructField('2018',types.DoubleType()),
    types.StructField('2019',types.DoubleType()),
    types.StructField('2020',types.DoubleType()),
    types.StructField('2021',types.DoubleType()),
    types.StructField('2022',types.DoubleType()),
    types.StructField('Estimates Start After',types.IntegerType())    
])

FDindex_schema = types.StructType([
    types.StructField('ifs', types.IntegerType()),
    types.StructField('code', types.StringType()),
    types.StructField('country', types.StringType()),
    types.StructField('imf_region', types.StringType()),
    types.StructField('imf_income', types.StringType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('FD', types.DoubleType()),
    types.StructField('FI', types.DoubleType()),
    types.StructField('FM', types.DoubleType()),
    types.StructField('FID',types.DoubleType()),
    types.StructField('FIA',types.DoubleType()),
    types.StructField('FIE', types.DoubleType()),
    types.StructField('FMD', types.DoubleType()),
    types.StructField('FMA', types.DoubleType()),
    types.StructField('FME', types.DoubleType())
])

"""
FD - financial development (normalized values)
 |---- FI - financial institutions --- FID (depth), FIA (access), FIE (efficiency)
 |---- FM - financial markets --- FMD (depth), FMA (access), FME (efficiency)
"""

def main():

    imf = spark.read.csv('../data/Canada-IMF.csv',schema=IMF_schema) # reading IMF Macroeconomic csv data
    fd = spark.read.csv('../data/FDindex-AllCountries.csv',schema=FDindex_schema) # reading FDindex csv data


    # to transpose a dataframe -> interchange rows & columns
    def TransposeDF(df, columns, pivotCol): 
        columnsValue = list(map(lambda x: str("'") + str(x) + str("',")  + str(x), columns))
        stackCols = ','.join(x for x in columnsValue)
        df_1 = df.selectExpr(pivotCol, "stack(" + str(len(columns)) + "," + stackCols + ")").select(pivotCol, "col0", "col1")
        final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1")))).withColumnRenamed("col0", pivotCol)
        return final_df
    
    
    ##################################### IMF Operations ###########################################################
    checkNull = imf.filter(imf['Subject Descriptor'].isNotNull() & imf['Units'].isNotNull())
    # fetch IMF data for the year range 2010 to 2018\
    imf1 = checkNull.select('Subject Descriptor','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019')
    # to drop rows with null fields and remove duplicates
    imf1 = imf1.distinct().dropna().dropDuplicates()
    df = imf1.toPandas() # pandas df
    df = df.drop(df.index[0])
    imf2 = spark.createDataFrame(df) # spark df

    # saving the output of imf1
    imf2.coalesce(1).write.csv('../OUTPUT-Folder/IMF_output',header=True,mode='overwrite')
    
    
    ##################################### FDindex Operations #######################################################
    checkNull1 = fd.filter(fd['ifs'].isNotNull() & fd['code'].isNotNull() & fd['country'].isNotNull())
    # select FD values only for Canada
    canada = checkNull1.where(checkNull1['country']=='Canada')
    canada = canada.select('year','FD','FI','FM','FID','FIA','FIE','FMD','FMA','FME')

    #fetch FD data for the last 8 years between 2010 and 2018
    fd1 = canada.where(canada['year'].between('2010','2018')).orderBy('year')
    fd_res = TransposeDF(fd1, ["FD", "FI", "FM", "FID", "FIA", "FIE", "FMD", "FMA", "FME"], "year")
    # saving the output of fd_res
    fd_res.coalesce(1).write.csv('../OUTPUT-Folder/FDindex_output',header=True,mode='overwrite')        


    ######### PENDING --- ML PREDICTION ###########
    # for imf2 -> preict for 2020, 2021
    # for fd_res -> predict for 2019, 2020, 2021

     	
if __name__ == '__main__':
    spark = SparkSession.builder.appName('IMF+FDindex Analysis').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()



  
