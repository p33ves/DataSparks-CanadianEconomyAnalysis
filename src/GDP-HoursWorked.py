import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import to_date,lit,year,avg,round,split,when
import datetime

#Both the Tables have data for Canada for industries and not province-wise

gdp_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType()),
    types.StructField('GEO', types.StringType()),
    types.StructField('DGUID', types.StringType()),
    types.StructField('Seasonal adjustment', types.StringType()),
    types.StructField('Prices',types.StringType()),
    types.StructField('North American Industry Classification System (NAICS)', types.StringType()),
    types.StructField('UOM', types.StringType()),
    types.StructField('UOM_ID', types.StringType()),
    types.StructField('SCALAR_FACTOR',types.StringType()),
    types.StructField('SCALAR_ID', types.StringType()),
    types.StructField('VECTOR', types.StringType()),
    types.StructField('COORDINATE', types.StringType()),
    types.StructField('VALUE', types.StringType()),
    types.StructField('STATUS', types.StringType()),
    types.StructField('SYMBOL', types.StringType()),
    types.StructField('TERMINATED', types.StringType()),
    types.StructField('DECIMALS', types.StringType()),
])

hours_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType()),
    types.StructField('GEO', types.StringType()),
    types.StructField('DGUID', types.StringType()),
	types.StructField('North American Industry Classification System (NAICS)', types.StringType()),
    types.StructField('Statistics', types.StringType()),
    types.StructField('UOM', types.StringType()),
    types.StructField('UOM_ID', types.StringType()),
    types.StructField('SCALAR_FACTOR',types.StringType()),
    types.StructField('SCALAR_ID', types.StringType()),
    types.StructField('VECTOR', types.StringType()),
    types.StructField('COORDINATE', types.StringType()),
    types.StructField('VALUE', types.StringType()),
    types.StructField('STATUS', types.StringType()),
    types.StructField('SYMBOL', types.StringType()),
    types.StructField('TERMINATED', types.StringType()),
    types.StructField('DECIMALS', types.StringType()),
])
def get_kv(data):
	return (data['REF_DATE'], (data['NAICS'], data['GDP_VALUE']))
	
def convert_industry(data1, data2):
	if(data1[0] == 'Wholesale trade' and data2[0] == 'Retail trade'):
		return ('Wholesale and retail trade', mean(int(data1[2]),int(data2[2])))
	else:
		return data1

def main():

#Processing GDP Data
gdp_df = spark.read.csv('/home/at/project/GDP.csv', schema=gdp_schema)

#filter out null values for required columns
gdp_notnull_df = gdp_df.filter(gdp_df['REF_DATE'].isNotNull() | gdp_df['GEO'].isNotNull() | gdp_df['VALUE'].isNotNull())

#filter out "Seasonally adjusted at annual rates" values only from 'Seasonal adjustment' column
gdp_seasonal_df = gdp_notnull_df.filter(gdp_notnull_df['Seasonal adjustment'] == lit('Seasonally adjusted at annual rates'))
gdp_annual_df = gdp_seasonal_df.filter(gdp_seasonal_df['Prices'] == lit('Chained (2012) dollars'))

#convert 'REF_DATE' to date type
gdp_date_df = gdp_annual_df.withColumn('REF_DATE', to_date(gdp_annual_df['REF_DATE'], 'yyyy-MM'))

#fetch data for the last 10 years
gdp_decade_df = gdp_date_df.where(gdp_date_df['REF_DATE'].between(datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2020-10-01','%Y-%m-%d')))

#Elimininate from 'NAICS' column all the values in [] to spearate out the industry names
gdp_filter = gdp_decade_df.withColumn('North American Industry Classification System (NAICS)', (split(gdp_decade_df['North American Industry Classification System (NAICS)'], "\[")[0]))

#fetch only required columns
gdp = gdp_filter.select('REF_DATE',(gdp_filter['North American Industry Classification System (NAICS)']).alias('NAICS'),(gdp_filter['VALUE']).alias('GDP_VALUE'))

#Converting NAICS into Indutry values and filtering out major industries.
gdp_rdd=gdp.replace(['Wholesale trade','Retail trade'],['Wholesale and retail trade','Wholesale and retail trade'])

gdp_final = gdp.groupBy(year('REF_DATE').alias('YEAR')).pivot('NAICS').agg(round(avg('GDP_VALUE'),2)).orderBy('YEAR')
result_cpi_df.write.csv('../Canada_CPI_output', header='true', mode='overwrite')





#Processing Actual Hours Worked Data
hours_df = spark.read.csv('/home/at/project/hours-worked.csv', schema=hours_schema)

#Filter Null rows if fields 'REF_DATE','GEO' or 'VALUE' is Null
hours_notnull_df = hours_df.filter(hours_df['REF_DATE'].isNotNull() | hours_df['GEO'].isNotNull() | hours_df['VALUE'].isNotNull())

#convert 'REF_DATE' to date type
hours_date_df = hours_notnull_df.withColumn('REF_DATE', to_date(hours_notnull_df['REF_DATE'], 'yyyy-MM'))


#fetch data for the last 10 years
hours_decade_df = hours_date_df.where(hours_date_df['REF_DATE'].between(datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2020-10-01','%Y-%m-%d')))

#Fetch only the 'Estimate' values for hours and ignore all standard error values
hours_est = hours_decade_df.where(hours_decade_df['Statistics'] == lit('Estimate'))

#Elimininate from 'NAICS' column all the values in [] to spearate out the industry names
hours_filter = hours_est.withColumn('North American Industry Classification System (NAICS)', (split(hours_est['North American Industry Classification System (NAICS)'], "\[")[0]))

#fetch only required columns
hours = hours_filter.select('REF_DATE',(hours_filter['North American Industry Classification System (NAICS)']).alias('NAICS'),(hours_filter['VALUE']).alias('HOURS_WORKED'))


#Agriculture
#Forestry, fishing, mining, quarrying, oil and gas
#avg and name 'Agriculture, forestry, fishing and hunting'

hours_rep=hours.withColumn('NAICS',functions.regexp_replace('NAICS',r'(Agriculture|Forestry)','Agriculture, forestry, fishing and hunting'))

#convert 'HOURS_WORKED' to int type
hours_int_df = hours_rep.withColumn('HOURS_WORKED', (hours_rep['HOURS_WORKED']).cast(types.IntegerType()))



hours_group=hours_int_df.groupBy('REF_DATE','NAICS').avg('HOURS_WORKED').orderBy('REF_DATE')

#Merge GDP and Hourly Hours worked tables based on 'REF_DATE'
gdp_hours_df = gdp_decade_df.join(hours_decade_df,['REF_DATE'],'inner')

if __name__ == '__main__':
    spark = SparkSession.builder.appName('CPI Analysis').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()