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

merch_trade_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType()),
    types.StructField('GEO', types.StringType()),
    types.StructField('DGUID', types.StringType()),
    types.StructField('Trade', types.StringType()),
    types.StructField('Basis', types.StringType()),
    types.StructField('Seasonal adjustment', types.StringType()),
    types.StructField('North American Product Classification System (NAPCS)',types.StringType()),
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

    gdp = spark.read.csv('../GDPreal.csv',schema=gdp_schema) #reading GDP Data csv
    MT = spark.read.csv('../MerchTrade.csv',schema=merch_trade_schema) #reading International Merchandise Trade Data csv

    
    ##################################### GDP Operations ##########################################################
    checkNull = gdp.filter(gdp['REF_DATE'].isNotNull() & gdp['VALUE'].isNotNull()).withColumn('REF_DATE', to_date(gdp['REF_DATE'], 'yyyy-MM'))
    # selection of relevant columns which are 'seasonally adjusted at annual rates'
    season = checkNull.where(checkNull['Seasonal adjustment']=='Seasonally adjusted at annual rates')
    seasonalGDP = season.select('REF_DATE','Seasonal adjustment','North American Industry Classification System (NAICS)','VALUE')

    #fetch gdp data for the last 20 years between Jan 2000 and Oct 2020
    duration = seasonalGDP.where(seasonalGDP['REF_DATE'].between(datetime.datetime.strptime('2000-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2020-10-01','%Y-%m-%d')))

    # to group by the year and NAICS, to get the gdp for the  industry in that year
    group1 = duration.groupby('REF_DATE','North American Industry Classification System (NAICS)').sum('VALUE')
    group1 = group1.withColumnRenamed('North American Industry Classification System (NAICS)','NAICS').withColumnRenamed('sum(VALUE)','Total GDP Value')

    #Cannabis production industry can be ignored as it doesnt contribute much to GDP 
    result = group1.where(group1['NAICS']!='Cannabis production [111C]').where(group1['NAICS']!='Cannabis production (unlicensed) [111CU]').where(group1['NAICS']!='Cannabis production (licensed) [111CL]')

    # eliminate industries with lower GDP per year
    GDP_res = result.where(result['Total GDP Value']>100)

    #################################################### Merch Trade Operations #########################################
    checkNull1 = MT.filter(MT['REF_DATE'].isNotNull() & MT['VALUE'].isNotNull()).withColumn('REF_DATE', to_date(MT['REF_DATE'], 'yyyy-MM'))
    # select only seasonally adjusted merch rates, and then select relevant columns
    season1 = checkNull1.where(checkNull1['Seasonal adjustment']=='Seasonally adjusted')
    seasonalMT = season1.select('REF_DATE','Trade','Basis','Seasonal adjustment','North American Product Classification System (NAPCS)','VALUE')

    #fetch MT data for the last 20 years between Jan 2000 and Oct 2020
    duration1 = seasonalMT.where(seasonalMT['REF_DATE'].between(datetime.datetime.strptime('2000-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2020-10-01','%Y-%m-%d')))

    # to group by the year and NAPCS, to get the Trade value
    groupMT = duration1.groupby('REF_DATE','North American Product Classification System (NAPCS)','Trade','Basis').sum('VALUE')
    groupMT2 = groupMT.withColumnRenamed('North American Product Classification System (NAPCS)','NAPCS').withColumnRenamed('sum(VALUE)','Total Merch Trade Value')
    groupMT3 = groupMT2.withColumnRenamed('REF_DATE','YEAR')

    # eliminate industries with lower MT values per year
    MT_res = groupMT3.where(groupMT2['Total Merch Trade Value']>100)

    # to save final results of GDP and MT operations in 2 different folders
    GDP_res.coalesce(1).write.csv('GDP_output', header='true', mode='overwrite') ### GDP_output-> REF_DATE, NAICS, Total GDP Value
    MT_res.coalesce(1).write.csv('MT_output', header='true', mode='overwrite') ### MT_output-> YEAR, NAPCS, Trade, Basis, Total Merch Trade Value

    ########### to join GDP and MT dataframes based on the 'REF_DATE' -------------> Final Goal (for visualization) ################
    final_res = GDP_res.join(MT_res, GDP_res.REF_DATE==MT_res.YEAR, "inner").drop(MT_res['YEAR']) # avoid duplication of "YEAR" column
    # select only the "Customs" Basis data
    final_df = final_res.where(final_res['Basis']=='Customs')
    # for each year, by month (between Jan 2000 and Aug 2020) -> Find the Avg GDP and Avg Merch Trade Value for all Trades (imports and exports)
    FINAL_df = final_df.groupby('REF_DATE').agg(avg('Total GDP Value').alias('Avg GDP Value'), avg('Total Merch Trade Value').alias('Avg Merch Trade Value')).orderBy('REF_DATE')
    FINAL_df.coalesce(1).write.csv('GDP+MT_output', header='true', mode='overwrite') ### FINAL_df-> REF_DATE, Avg GDP Value, Avg Merch Trade Value
    	
if __name__ == '__main__':
    spark = SparkSession.builder.appName('GDP+MT Analysis').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()



    
