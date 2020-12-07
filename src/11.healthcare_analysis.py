import os, datetime, json
import boto3

from pyspark.sql import SparkSession, types, functions
from pyspark.sql.functions import to_date, avg

IN_PATH = "s3://mysparks/data/clean/statcan/"
OUT_PATH = "s3://mysparks/OUTPUT-Folder/"
#IN_PATH = "../data/clean/statcan/"
#OUT_PATH = "../OUTPUT-Folder/"

SCHEMA_PATH = "schema/statcan/"
gdp_id = "36100434"
cpi_id = "18100004"
cc_id = "13100781"
retail_id = "20100008"

s3_obj = boto3.client('s3')
s3_gdp_obj = s3_obj.get_object(Bucket='mysparks', Key=SCHEMA_PATH + "gdp.json")
s3_gdp_data = s3_gdp_obj['Body'].read().decode('utf-8')
gdp_schema = json.loads(s3_gdp_data)

s3_cpi_obj = s3_obj.get_object(Bucket='mysparks', Key=SCHEMA_PATH + cpi_id + ".json")
s3_cpi_data = s3_cpi_obj['Body'].read().decode('utf-8')
cpi_schema = json.loads(s3_cpi_data)

s3_cc_obj = s3_obj.get_object(Bucket='mysparks', Key=SCHEMA_PATH + "covid_cases.json")
s3_cc_data = s3_cc_obj['Body'].read().decode('utf-8')
cc_schema = json.loads(s3_cc_data)

s3_retail_obj = s3_obj.get_object(Bucket='mysparks', Key=SCHEMA_PATH + "retailsales_canada.json")
s3_retail_data = s3_retail_obj['Body'].read().decode('utf-8')
retail_schema = json.loads(s3_retail_data)

os.makedirs(OUT_PATH, exist_ok=True)

#gdp_schema = json.load(open("../schema/statcan/" + gdp_id + ".json"))
#cpi_schema = json.load(open("../schema/statcan/" + cpi_id + ".json"))
#cc_schema = json.load(open("../schema/statcan/" + cc_id + ".json"))
#retail_schema = json.load(open("../schema/statcan/" + retail_id + ".json"))

def main():
    gdp = spark.read.csv(OUT_PATH + 'GDP_output/*.csv',
                         schema=types.StructType.fromJson(gdp_schema))  # reading GDP Data csv
    cpi = spark.read.csv(IN_PATH + cpi_id + '/*.csv',
                         schema=types.StructType.fromJson(cpi_schema))  # reading CPI Data csv
    covid_cases = spark.read.csv(OUT_PATH + 'covid_cases/*.csv',
                                 schema=types.StructType.fromJson(cc_schema))  # reading Covid Cases Data csv
    retail = spark.read.csv(OUT_PATH + 'Retail2_output/*.csv',
                            schema=types.StructType.fromJson(retail_schema))  # reading retail Data csv

    # region GDP data selection
    select_fields = gdp.filter((functions.year(gdp['REF_DATE']) == 2020) & (gdp['NAICS'].contains('care')))
    care_fields = select_fields.withColumn('Temp', functions.concat(
        functions.lit("GDP of "), select_fields['NAICS'], functions.lit(" x10^6 (in CAD)")))
    healthcare_gdp = care_fields.groupby(care_fields['REF_DATE']) \
        .pivot("Temp").agg(functions.first(care_fields["Total GDP Value"]))
    # endregion

    # region CPI data selection
    select_fields = cpi.filter((functions.year(cpi['REF_DATE']) == 2020) & (cpi['GEO'] == "Canada") &
                               ((cpi['Products and product groups'].contains("health")) |
                                (cpi['Products and product groups'] == "Medicinal and pharmaceutical products")))
    health_fields = select_fields.withColumn('Temp', functions.concat(
        functions.lit("CPI of "), select_fields['Products and product groups']))
    healthcare_cpi = health_fields.groupby(health_fields['REF_DATE']) \
        .pivot("Temp").agg(functions.first(health_fields['VALUE']))
    # endregion

    # region Retail data select
    healthcare_retail = retail.filter((functions.year(retail['REF_DATE']) == 2020)) \
        .select(retail['REF_DATE'], retail["Health and personal care stores [446]"]
                .alias("Retail Trade sales for - Health and personal care stores x10^3 (in CAD)"))
    # endregion

    # region covid_cases
    new_cases = covid_cases.groupby(covid_cases['Episode month']).count()
    recovered_cases = covid_cases.groupby(covid_cases['Recovery month']).count()
    deaths = covid_cases.filter(covid_cases['Death']).groupby(covid_cases['Episode month']).count()
    hospital_statuses_cases = covid_cases.groupby(covid_cases['Episode month']) \
        .pivot('Hospital status', ["Hospitalized and in ICU", "Hospitalized, but not in ICU",
                                   "Not hospitalized", "Not Stated/Unknown"]).count()
    covid_trend = new_cases.select(new_cases['Episode month'].alias("Month"),
                                   new_cases['count'].alias("New cases"))
    # endregion

    # FINAL_df-> REF_DATE, Avg GDP Value, Avg Merch Trade Value
    # with open(SCHEMA_PATH + "GDP+MT_output.json", 'w') as out_file:
    #     out_file.write(FINAL_df.schema.json())
    # FINAL_df.coalesce(1).\
    #     write.csv('../OUTPUT-Folder/GDP+MT_output', header='true', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Health care Analysis').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
