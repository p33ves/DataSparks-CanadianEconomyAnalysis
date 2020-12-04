import os, datetime, json
from pyspark.sql import SparkSession, types, functions
from pyspark.sql.functions import to_date, avg

IN_PATH = "../data/clean/statcan/"
OUT_PATH = "../OUTPUT-Folder/"
SCHEMA_PATH = "../schema/processed/"
gdp_id = "36100434"
cpi_id = "18100004"
cc_id = "13100781"
retail_id = "20100008"
os.makedirs(OUT_PATH, exist_ok=True)
gdp_schema = json.load(open(SCHEMA_PATH + "gdp.json"))
cpi_schema = json.load(open(SCHEMA_PATH + cpi_id + ".json"))
cc_schema = json.load(open(SCHEMA_PATH + "covid_cases.json"))
retail_schema = json.load(open(SCHEMA_PATH + "retailsales_canada.json"))


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
