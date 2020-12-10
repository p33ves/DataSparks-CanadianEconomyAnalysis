import os
import json
import sys
import datetime
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import to_date, lit, year, avg


# Forming table for GDP prediction

IN_PATH = "s3://mysparks/OUTPUT-Folder/"
OUT_PATH = "s3://mysparks/OUTPUT-Folder/"
#IN_PATH = "/home/at/project/output/"
#OUT_PATH = "/home/at/project/output/out/"


os.makedirs(OUT_PATH, exist_ok=True)
#exp_schema = json.load(open("../schema/statcan/" + exp_id + ".json"))
hours_schema = types.StructType([
	types.StructField('YEAR', types.StringType()),
	types.StructField('NAICS', types.StringType()),
	types.StructField('HOURS_WORKED', types.DecimalType()),
	types.StructField('GDP_VALUE', types.DecimalType()),
	types.StructField('labour_productivity', types.DecimalType()),
])

cpi_schema = types.StructType([
	types.StructField('YEAR', types.StringType()),
	types.StructField('Alberta', types.DecimalType()),
	types.StructField('British Columbia', types.DecimalType()),
	types.StructField('Canada', types.DecimalType()),
	types.StructField('Manitoba', types.DecimalType()),
	types.StructField('New Brunswick', types.DecimalType()),
	types.StructField('Newfoundland and Labrador', types.DecimalType()),
	types.StructField('Nova Scotia', types.DecimalType()),
	types.StructField('Ontario', types.DecimalType()),
	types.StructField('Prince Edward Island', types.DecimalType()),
	types.StructField('Quebec', types.DecimalType()),
	types.StructField('Saskatchewan', types.DecimalType()),
])
mt_schema = types.StructType([
	types.StructField('REF_DATE', types.StringType()),
	types.StructField('Avg GDP Value', types.DecimalType()),
	types.StructField('Avg Merch Trade Value', types.DecimalType()),
])
exp_schema = types.StructType([
	types.StructField('YEAR', types.StringType()),
	types.StructField('household_expenditure*(10^6)', types.DecimalType())
])
def main():
	########################Hours worked##################################
	hours_df = spark.read.csv(IN_PATH +'GDP_hours_worked_output/' + '*.csv', schema=hours_schema)

	# filter All Industries
	hours_ind = hours_df.filter(hours_df['NAICS'] == lit('All industries'))
	
	hours = hours_ind.select(hours_ind['YEAR'],(hours_ind['HOURS_WORKED']*1000).alias('Hours_Worked'),hours_ind['labour_productivity'])

	########################CPI##################################
	cpi_df = spark.read.csv(IN_PATH + 'CPI_output/' + '*.csv', schema=cpi_schema)

	cpi_can = cpi_df.select(cpi_df['YEAR'],(cpi_df['Canada']).alias('CPI'))

	cpi = cpi_can.filter(cpi_can['YEAR'].isNotNull())
	
	########################Merch Trade Data##################################
	mt_df = spark.read.csv(IN_PATH + 'GDP+MT_output/' + '*.csv', schema=mt_schema)
	
	# Get yearly average data for International Merchant Trade values and GDP values
	mt_avg = mt_df.groupBy(year('REF_DATE').alias('YEAR')).avg().orderBy('YEAR')
	
	mt_rename = mt_avg.withColumnRenamed('avg(Avg GDP Value)', 'GDP_millions')\
						.withColumnRenamed('avg(Avg Merch Trade Value)', 'Merchant_Trade_millions')
	mt = mt_rename.filter(mt_rename['YEAR'].isNotNull())
		
	########################Household consumption Data##################################
	exp_df = spark.read.csv(IN_PATH + 'household_consumption_output/' + '*.csv',
							schema=exp_schema)
							
	exp_rename = exp_df.withColumnRenamed('household_expenditure*(10^6)','household_expenditure_millions')
	
	exp = exp_rename.filter(exp_rename['YEAR'].isNotNull())
	
	gdp = hours.join(cpi, ['YEAR'])\
				.join(mt, ['YEAR'])\
				.join(exp, ['YEAR'])

	gdp.coalesce(1).write.csv(OUT_PATH + 'gdp_pred_input', header='true', mode='overwrite')


if __name__ == '__main__':
	spark = SparkSession.builder.appName('Canada Household expenditure').getOrCreate()
	assert spark.version >= '2.4'  # make sure we have Spark 2.4+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main()
