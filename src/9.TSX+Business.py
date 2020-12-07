import json
import os
import boto3

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import to_date, avg, year

IN_PATH = "s3://mysparks/data/clean/statcan/"
OUT_PATH = "s3://mysparks/OUTPUT-Folder/"
SCHEMA_PATH = "schema/statcan/"
tsx_id = "10100125"
bus_id = "33100111"

s3_obj = boto3.client('s3')
s3_tsx_obj = s3_obj.get_object(Bucket='mysparks', Key=SCHEMA_PATH + tsx_id + ".json")
s3_tsx_data = s3_tsx_obj['Body'].read().decode('utf-8')
TSX_schema = json.loads(s3_tsx_data)

s3_bus_obj = s3_obj.get_object(Bucket='mysparks', Key=SCHEMA_PATH + bus_id + ".json")
s3_bus_data = s3_bus_obj['Body'].read().decode('utf-8')
Business_schema = json.loads(s3_bus_data)

os.makedirs(OUT_PATH, exist_ok=True)
#TSX_schema = json.load(open("../schema/statcan/" + tsx_id + ".json"))
#Business_schema = json.load(open("../schema/statcan/" + bus_id + ".json"))

def main():
    tsx = spark.read.csv(IN_PATH + tsx_id + '/*.csv',
                         schema=types.StructType.fromJson(TSX_schema))  # reading 'TSX' csv Data
    buss = spark.read.csv(IN_PATH + bus_id + '/*.csv',
                          schema=types.StructType.fromJson(Business_schema))  # reading 'BusinessIndicators' csv Data

    ############################################### TSX Operations #################################################
    checkNull = tsx.filter(tsx['REF_DATE'].isNotNull() & tsx['VALUE'].isNotNull()).withColumn('REF_DATE',
                                                                                              to_date(tsx['REF_DATE'],
                                                                                                      'yyyy-MM'))
    # to select non-null index values for TSX
    tsx1 = checkNull.where(checkNull['UOM'] == 'Index').where(checkNull['VALUE'] > 0)
    tsx1 = tsx1.groupby('REF_DATE', 'Toronto Stock Exchange Statistics').sum('VALUE')
    tsx2 = tsx1.withColumnRenamed('sum(VALUE)', 'Total Stock Value')
    res_tsx = tsx2.where(tsx2['Total Stock Value'] > 100).orderBy(
        'REF_DATE')  # Discard low stock values that do not contribute to industrial GDP
    res_tsx.coalesce(1).write.csv(OUT_PATH + 'TSX_output', header=True,
                                  mode='overwrite')  # Output -> REF_DATE, Toronto Stock Exchange Statistics, Total Stock Value

    # To find net worth of top 60 Canadian Companies Stock
    res_tsx1 = tsx2.where(
        tsx2['Toronto Stock Exchange Statistics'] == "Standard and Poor's/Toronto Stock Exchange 60 Index")
    res_tsx1 = res_tsx1.withColumnRenamed('Total Stock Value', 'Total TSX 60 Value').orderBy('REF_DATE')
    res_tsx1 = res_tsx1.select('REF_DATE', 'Total TSX 60 Value')
    res_tsx1.coalesce(1).write.csv(OUT_PATH + 'TSX60_output', header=True,
                                   mode='overwrite')  # Output -> REF_DATE, Total TSX 60 Value

    ########################################### Business Indicators Operations #####################################
    checkNull1 = buss.filter(buss['REF_DATE'].isNotNull() & buss['VALUE'].isNotNull()).withColumn('REF_DATE', to_date(
        buss['REF_DATE'], 'yyyy-MM'))
    # selecting business indicator for year 2010, which acts as basis for the decade 2010 to 2020
    checkNull1 = checkNull1.where(checkNull1['REF_DATE'].between(
          datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2010-12-01', '%Y-%m-%d')))
    # to select valid Smoothed Composite index values for indicators
    buss1 = checkNull1.where(checkNull1['VALUE'] > 0).where(checkNull1['Composite index'] == 'Smoothed')
    buss1 = checkNull1.groupby('REF_DATE', 'Leading Indicators').sum('VALUE').withColumnRenamed('REF_DATE', 'YEAR')
    res_buss = buss1.withColumnRenamed('sum(VALUE)', 'Business Profit Factor').orderBy('YEAR')
    res_buss.coalesce(1).write.csv(OUT_PATH + 'BIndicator_output', header='True', mode='overwrite')

    ################################ Relation between TSX stock and Business Indicators #################################
    r_tsx = res_tsx.groupby(year('REF_DATE').alias('REF_DATE')).agg(
        avg('Total Stock Value').alias('Avg Yearly TSX Stock'))
    final_res = r_tsx.select('REF_DATE', 'Avg Yearly TSX Stock').orderBy('REF_DATE')
    final_res.coalesce(1).write.csv(OUT_PATH + 'TSX-yearly', header=True, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('TSX+BusinessIndicators').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
