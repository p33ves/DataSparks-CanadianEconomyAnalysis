import os
import json
import sys
import datetime
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import to_date, lit, year, broadcast


# Gdp analysis of top 10 countries

IN_PATH = "s3://mysparks/data/gdp/"
OUT_PATH = "s3://mysparks/OUTPUT-Folder/gdp/"
#IN_PATH = "/home/at/project/gdp/GDPCountries/"
#OUT_PATH = "/home/at/project/gdp/out/"

os.makedirs(OUT_PATH, exist_ok=True)

can_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('MKTGDPCAA646NWDB', types.StringType()),
])

us_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('FYGDP', types.StringType()),
])
ind_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('MKTGDPINA646NWDB', types.StringType()),
])

bzl_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('MKTGDPBRA646NWDB', types.StringType()),
])

chn_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('MKTGDPCNA646NWDB', types.StringType()),
])
frn_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('MKTGDPFRA646NWDB', types.StringType()),
])
ger_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('MKTGDPDEA646NWDB', types.StringType()),
])
itl_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('MKTGDPITA646NWDB', types.StringType()),
])
jap_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('MKTGDPJPA646NWDB', types.StringType()),
])
uk_schema = types.StructType([
	types.StructField('DATE', types.StringType()),
	types.StructField('MKTGDPGBA646NWDB', types.StringType()),
])

def main():
	########################Processing Canada Data##################################
	can_df = spark.read.csv(IN_PATH + 'Canada.csv',
							schema=can_schema)

	can_gdp = can_df.withColumnRenamed('MKTGDPCAA646NWDB','Canada')
	
	# filter out null values for required columns
	can_notnull_df = can_gdp.filter(
		can_gdp['DATE'].isNotNull() | can_gdp['Canada'].isNotNull())

	# fetch data for the last 20 years
	can_decade_df = can_notnull_df.where(
		can_notnull_df['DATE'].between(datetime.datetime.strptime('01-01-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('01-01-2019', '%d-%m-%Y')))

	# Convert GDP to billions
	can_gdpb_df = can_decade_df.withColumn(
		'Canada', can_decade_df['Canada']/(10**9))
	
	# convert 'REF_DATE' to date type
	can_date = can_gdpb_df.withColumn('DATE', to_date(can_gdpb_df['DATE'], 'yyyy-MM'))

	#Take only the year
	can = can_date.withColumn('DATE', year(can_date['DATE']))
	
	########################Processing United States Data##################################
	us_df = spark.read.csv(IN_PATH + 'US.csv',
							schema=us_schema)

	us_gdp = us_df.withColumnRenamed('FYGDP','US')
	
	# filter out null values for required columns
	us_notnull_df = us_gdp.filter(
		us_gdp['DATE'].isNotNull() | us_gdp['US'].isNotNull())
	

	# fetch data for the last 20 years
	us_decade_df = us_notnull_df.where(
		us_notnull_df['DATE'].between(datetime.datetime.strptime('30-09-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('30-09-2019', '%d-%m-%Y')))
		
	# convert 'REF_DATE' to date type
	us_date = us_decade_df.withColumn('DATE', to_date(us_decade_df['DATE'], 'yyyy-MM'))
	
	
	#Take only the year
	us = us_date.withColumn('DATE', year(us_date['DATE']))
	
	########################Processing India Data##################################
	ind_df = spark.read.csv(IN_PATH + 'India.csv',
							schema=ind_schema)

	ind_gdp = ind_df.withColumnRenamed('MKTGDPINA646NWDB','India')
	
	# filter out null values for required columns
	ind_notnull_df = ind_gdp.filter(
		ind_gdp['DATE'].isNotNull() | ind_gdp['India'].isNotNull())

	# fetch data for the last 20 years
	ind_decade_df = ind_notnull_df.where(
		ind_notnull_df['DATE'].between(datetime.datetime.strptime('01-01-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('01-01-2019', '%d-%m-%Y')))

	# Convert GDP to billions
	ind_gdpb_df = ind_decade_df.withColumn(
		'India', ind_decade_df['India']/(10**9))

	# convert 'REF_DATE' to date type
	ind_date = ind_gdpb_df.withColumn('DATE', to_date(ind_gdpb_df['DATE'], 'yyyy-MM'))
	
	#Take only the year
	ind = ind_date.withColumn('DATE', year(ind_date['DATE']))
	
	########################Processing Brazil Data##################################
	bzl_df = spark.read.csv(IN_PATH + 'Brazil.csv',
							schema=bzl_schema)

	bzl_gdp = bzl_df.withColumnRenamed('MKTGDPBRA646NWDB','Brazil')
	
	# filter out null values for required columns
	bzl_notnull_df = bzl_gdp.filter(
		bzl_gdp['DATE'].isNotNull() | bzl_gdp['Brazil'].isNotNull())

	# fetch data for the last 20 years
	bzl_decade_df = bzl_notnull_df.where(
		bzl_notnull_df['DATE'].between(datetime.datetime.strptime('01-01-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('01-01-2019', '%d-%m-%Y')))

	# Convert GDP to billions
	bzl_gdpb_df = bzl_decade_df.withColumn(
		'Brazil', bzl_decade_df['Brazil']/(10**9))
	
	# convert 'REF_DATE' to date type
	bzl_date = bzl_gdpb_df.withColumn('DATE', to_date(bzl_gdpb_df['DATE'], 'yyyy-MM'))
		
	#Take only the year
	bzl = bzl_date.withColumn('DATE', year(bzl_date['DATE']))
	
	########################Processing China Data##################################
	chn_df = spark.read.csv(IN_PATH + 'China.csv',
							schema=chn_schema)

	chn_gdp = chn_df.withColumnRenamed('MKTGDPCNA646NWDB','China')
	
	# filter out null values for required columns
	chn_notnull_df = chn_gdp.filter(
		chn_gdp['DATE'].isNotNull() | chn_gdp['China'].isNotNull())

	# fetch data for the last 20 years
	chn_decade_df = chn_notnull_df.where(
		chn_notnull_df['DATE'].between(datetime.datetime.strptime('01-01-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('01-01-2019', '%d-%m-%Y')))

	# Convert GDP to billions
	chn_gdpb_df = chn_decade_df.withColumn(
		'China', chn_decade_df['China']/(10**9))
	
	# convert 'REF_DATE' to date type
	chn_date = chn_gdpb_df.withColumn('DATE', to_date(chn_gdpb_df['DATE'], 'yyyy-MM'))
			
	#Take only the year
	chn = chn_date.withColumn('DATE', year(chn_date['DATE']))
	
	########################Processing France Data##################################
	frn_df = spark.read.csv(IN_PATH + 'France.csv',
							schema=frn_schema)

	frn_gdp = frn_df.withColumnRenamed('MKTGDPFRA646NWDB','France')
	
	# filter out null values for required columns
	frn_notnull_df = frn_gdp.filter(
		frn_gdp['DATE'].isNotNull() | frn_gdp['France'].isNotNull())

	# fetch data for the last 20 years
	frn_decade_df = frn_notnull_df.where(
		frn_notnull_df['DATE'].between(datetime.datetime.strptime('01-01-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('01-01-2019', '%d-%m-%Y')))

	# Convert GDP to billions
	frn_gdpb_df = frn_decade_df.withColumn(
		'France', frn_decade_df['France']/(10**9))
		
	# convert 'REF_DATE' to date type
	frn_date = frn_gdpb_df.withColumn('DATE', to_date(frn_gdpb_df['DATE'], 'yyyy-MM'))
				
	#Take only the year
	frn = frn_date.withColumn('DATE', year(frn_date['DATE']))
	
	########################Processing Germany Data##################################
	ger_df = spark.read.csv(IN_PATH + 'Germany.csv',
							schema=ger_schema)

	ger_gdp = ger_df.withColumnRenamed('MKTGDPDEA646NWDB','Germany')
	
	# filter out null values for required columns
	ger_notnull_df = ger_gdp.filter(
		ger_gdp['DATE'].isNotNull() | ger_gdp['Germany'].isNotNull())

	# fetch data for the last 20 years
	ger_decade_df = ger_notnull_df.where(
		ger_notnull_df['DATE'].between(datetime.datetime.strptime('01-01-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('01-01-2019', '%d-%m-%Y')))

	# Convert GDP to billions
	ger_gdpb_df = ger_decade_df.withColumn(
		'Germany', ger_decade_df['Germany']/(10**9))
	
	# convert 'REF_DATE' to date type
	ger_date = ger_gdpb_df.withColumn('DATE', to_date(ger_gdpb_df['DATE'], 'yyyy-MM'))
					
	#Take only the year
	ger = ger_date.withColumn('DATE', year(ger_date['DATE']))
	
	########################Processing Italy Data##################################
	itl_df = spark.read.csv(IN_PATH + 'Italy.csv',
							schema=itl_schema)

	itl_gdp = itl_df.withColumnRenamed('MKTGDPITA646NWDB','Italy')
	
	# filter out null values for required columns
	itl_notnull_df = itl_gdp.filter(
		itl_gdp['DATE'].isNotNull() | itl_gdp['Italy'].isNotNull())

	# fetch data for the last 20 years
	itl_decade_df = itl_notnull_df.where(
		itl_notnull_df['DATE'].between(datetime.datetime.strptime('01-01-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('01-01-2019', '%d-%m-%Y')))

	# Convert GDP to billions
	itl_gdpb_df = itl_decade_df.withColumn(
		'Italy', itl_decade_df['Italy']/(10**9))
	
	# convert 'REF_DATE' to date type
	itl_date = itl_gdpb_df.withColumn('DATE', to_date(itl_gdpb_df['DATE'], 'yyyy-MM'))
					
	#Take only the year
	itl = itl_date.withColumn('DATE', year(itl_date['DATE']))
		
	########################Processing Japan Data##################################
	jap_df = spark.read.csv(IN_PATH + 'Japan.csv',
							schema=jap_schema)

	jap_gdp = jap_df.withColumnRenamed('MKTGDPJPA646NWDB','Japan')
	
	# filter out null values for required columns
	jap_notnull_df = jap_gdp.filter(
		jap_gdp['DATE'].isNotNull() | jap_gdp['Japan'].isNotNull())

	# fetch data for the last 20 years
	jap_decade_df = jap_notnull_df.where(
		jap_notnull_df['DATE'].between(datetime.datetime.strptime('01-01-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('01-01-2019', '%d-%m-%Y')))

	# Convert GDP to billions
	jap_gdpb_df = jap_decade_df.withColumn(
		'Japan', jap_decade_df['Japan']/(10**9))
		
	# convert 'REF_DATE' to date type
	jap_date = jap_gdpb_df.withColumn('DATE', to_date(jap_gdpb_df['DATE'], 'yyyy-MM'))
						
	#Take only the year
	jap = jap_date.withColumn('DATE', year(jap_date['DATE']))
		
	########################Processing UK Data##################################
	uk_df = spark.read.csv(IN_PATH + 'UK.csv',
							schema=uk_schema)

	uk_gdp = uk_df.withColumnRenamed('MKTGDPGBA646NWDB','UK')
	
	# filter out null values for required columns
	uk_notnull_df = uk_gdp.filter(
		uk_gdp['DATE'].isNotNull() | uk_gdp['UK'].isNotNull())

	# fetch data for the last 20 years
	uk_decade_df = uk_notnull_df.where(
		uk_notnull_df['DATE'].between(datetime.datetime.strptime('01-01-1999', '%d-%m-%Y'),
										   datetime.datetime.strptime('01-01-2019', '%d-%m-%Y')))

	# Convert GDP to billions
	uk_gdpb_df = uk_decade_df.withColumn(
		'UK', uk_decade_df['UK']/(10**9))

	# convert 'REF_DATE' to date type
	uk_date = uk_gdpb_df.withColumn('DATE', to_date(uk_gdpb_df['DATE'], 'yyyy-MM'))
							
	#Take only the year
	uk = uk_date.withColumn('DATE', year(uk_date['DATE']))
		
	#######Joining Datframes###########
	gdp = can.join(broadcast(us), ['DATE'])\
			 .join(broadcast(ind),['DATE'])\
			 .join(broadcast(bzl),['DATE'])\
			 .join(broadcast(chn),['DATE'])\
			 .join(broadcast(frn),['DATE'])\
			 .join(broadcast(ger),['DATE'])\
			 .join(broadcast(itl),['DATE'])\
			 .join(broadcast(jap),['DATE'])\
			 .join(broadcast(uk),['DATE'])

	gdp.coalesce(1).write.csv(OUT_PATH + 'gdp_countries_output', header='true', mode='overwrite')


if __name__ == '__main__':
	spark = SparkSession.builder.appName('Comparing GDPs of top 10 countries').getOrCreate()
	assert spark.version >= '2.4'  # make sure we have Spark 2.4+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main()
