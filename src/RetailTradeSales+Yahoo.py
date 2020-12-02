import sys, datetime
from operator import add
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import to_date, round, avg, year

retail_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType()),
    types.StructField('GEO', types.StringType()),
    types.StructField('DGUID', types.StringType()),
    types.StructField('North American Industry Classification System (NAICS)', types.StringType()),
    types.StructField('Adjustments', types.StringType()),
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

yahoo_schema = types.StructType([
    types.StructField('REF_DATE', types.StringType()),
    types.StructField('Open', types.DoubleType()),
    types.StructField('High', types.DoubleType()),
    types.StructField('Low', types.DoubleType()),
    types.StructField('Close', types.DoubleType()),
    types.StructField('Adj Close', types.DoubleType()),
    types.StructField('Volume', types.DoubleType()),
]) 


def main():

    retail = spark.read.csv('../data/clean/statcan/20100008/Retail-trade-sales.csv',schema=retail_schema) #reading 'RetailTradeSales' csv Data 
    yahoo = spark.read.csv('../data/YahooFinance.csv',schema=yahoo_schema) #reading seasonal stock prices from 'YahooFinance' csv Data
    

    ############################ Retail Trade Operations #############################
    notnull = retail.filter(retail['REF_DATE'].isNotNull() | retail['GEO'].isNotNull() | retail['VALUE'].isNotNull()).withColumn('REF_DATE', to_date(retail['REF_DATE'], 'yyyy-MM'))

    #filter out only "Seasonally adjusted" items, according to seasonal trading
    seasonal = notnull.filter(notnull['Adjustments']=='Seasonally adjusted')

    #fetch retail data for the last 10 years between Jan 2010 and Oct 2020
    duration = seasonal.where(seasonal['REF_DATE'].between(datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2020-10-01','%Y-%m-%d')))

    #Taking only the provinces for case-1, consider entire Canada for Case-2
    province_df = duration.filter(~duration['GEO'].contains(',')).filter(duration['GEO']!='Canada')
    canada_df = duration.filter(duration['GEO']=='Canada')

    # Case1 --- To find avg seasonal(monthly) Retail Trade values for every Province in Canada
    # Case2 --- To find avg seasonal(monthly) Retail Trade values per industry for entire Canada
    retail1 = province_df.groupby('REF_DATE').pivot('GEO').agg(avg('VALUE')).orderBy('REF_DATE')
    retail2 = canada_df.groupby('REF_DATE').pivot('North American Industry Classification System (NAICS)').agg(avg('VALUE')).orderBy('REF_DATE')
    # In Case2, we can drop the 3 columns with null fields (dont contribute to Trade) -> Cannabis stores, Department stores, Other general merchandise stores
    retail2 = retail2.drop('Cannabis stores [453993]','Department stores [4521]','Other general merchandise stores [4529]')
    
    ### Retail_df output ###
    retail1.coalesce(1).write.csv('../OUTPUT-Folder/Retail1_output',header=True,mode='overwrite') # Province-wise Retail Trade values
    retail2.coalesce(1).write.csv('../OUTPUT-Folder/Retail2_output',header=True,mode='overwrite') # Industry-wise Retail Trade values for all provinces 



    ############################ Yahoo Operations ###################################
    # Seasonal (monthly) calculations
    checkNull = yahoo.filter(yahoo['REF_DATE'].isNotNull()).withColumn('REF_DATE', to_date(yahoo['REF_DATE'], 'yyyy-MM'))

    #fetch yahoo stock data for the last 10 years between Jan 2010 and Oct 2020
    duration1 = checkNull.where(checkNull['REF_DATE'].between(datetime.datetime.strptime('2010-01-01', '%Y-%m-%d'), datetime.datetime.strptime('2020-10-01','%Y-%m-%d')))
    
    # finding the avg monthly "highest" and "lowest" stock prices, and the Avg number of stocks traded
    yahoo1 = duration1.groupby('REF_DATE').agg(avg('Volume'), avg('High').alias('Avg Highest Stock'), avg('Low').alias('Avg Lowest Stock')).orderBy('REF_DATE')
    yahoo1 = yahoo1.withColumnRenamed('avg(Volume)','Avg Stock Traded')

    ### Yahoo_df output ###
    yahoo1.coalesce(1).write.csv('../OUTPUT-Folder/Yahoo_output',header=True,mode='overwrite') # Yahoo_output -> REF_DATE, Avg Volume, Avg Highest Stock, Avg Lowest Stock


    ######################### Merging Retail Trades Sales with corresponding Stocks traded (Yahoo finance) ##########
    TotalIndustryTrade = retail2.withColumn('TotalRetailTradePrice', sum(col(x) for x in retail2.columns[1:])) # to find the total Retail Trade values for all industries for that 'REF_DATE'
    final_res = TotalIndustryTrade.join(yahoo1, TotalIndustryTrade.REF_DATE==yahoo1.REF_DATE, "inner").drop(yahoo1['REF_DATE'])
    final_res1 = final_res.select('REF_DATE','TotalRetailTradePrice','Avg Stock Traded','Avg Highest Stock','Avg Lowest Stock').orderBy('REF_DATE')
    final_res1.coalesce(1).write.csv('../OUTPUT-Folder/Retail+YahooStock',header=True,mode='overwrite') # Retail+YahooStock --> REF_DATE, TotalRetailTradePrice, AvgStockTraded, AvgHighestStock, AvgLowestStock

    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('Retail+Yahoo Analysis').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()



  
