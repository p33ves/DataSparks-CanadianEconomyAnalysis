import sys, datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import to_date, round, lit, avg, year

gdp_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType()),
    types.StructField('GEO', types.StringType()),
    types.StructField('DGUID', types.StringType()),
    types.StructField('Seasonal adjustment', types.StringType()),
    types.StructField('Prices',types.StringType()),
    types.StructField('North American Industry Classification System (NAICS)', types.StringType()),
    types.StructField('UOM', types.StringType()),
    types.StructField('UOM_ID', types.IntegerType()),
    types.StructField('SCALAR_FACTOR',types.StringType()),
    types.StructField('SCALAR_ID', types.IntegerType()),
    types.StructField('VECTOR', types.StringType()),
    types.StructField('COORDINATE', types.DoubleType()),
    types.StructField('VALUE', types.DoubleType()),
    types.StructField('STATUS', types.StringType()),
    types.StructField('SYMBOL', types.StringType()),
    types.StructField('TERMINATED', types.StringType()),
    types.StructField('DECIMALS', types.IntegerType()),
])

def main():

    gdp = spark.read.csv('../GDPreal.csv',schema=gdp_schema) #reading csv
    
    checkNull = gdp.filter(gdp['REF_DATE'].isNotNull() & gdp['VALUE'].isNotNull()).withColumn('REF_DATE', to_date(gdp['REF_DATE'], 'yyyy-MM'))
    # selection of relevant columns
    seasonalGDP = checkNull.select('REF_DATE','Seasonal adjustment','North American Industry Classification System (NAICS)','VALUE')

    #fetch gdp data for the last 20 years between Jan 2000 and Oct 2020
    duration = seasonalGDP.where(seasonalGDP['REF_DATE'].between(datetime.datetime.strptime('2000-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2020-10-01','%Y-%m-%d')))

    # to group by the year and NAICS, to get the gdp for the  industry in that year
    group1 = duration.groupby('REF_DATE','North American Industry Classification System (NAICS)').sum('VALUE')
    group1 = group1.withColumnRenamed('North American Industry Classification System (NAICS)','NAICS').withColumnRenamed('sum(VALUE)','Total GDP Value')

    #Cannabis production industry can be ignored as it doesnt contribute much to GDP 
    result = group1.where(group1['NAICS']!='Cannabis production [111C]').where(group1['NAICS']!='Cannabis production (unlicensed) [111CU]').where(group1['NAICS']!='Cannabis production (licensed) [111CL]')
    result.coalesce(1).write.csv('../GDP_output', header='true', mode='overwrite') #.coalesce(1)
	
if __name__ == '__main__':
    spark = SparkSession.builder.appName('GDP Analysis').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()


