import json
import os
import boto3

from pyspark.sql import SparkSession, types, dataframe
from pyspark.sql.functions import when, first

IN_PATH = "s3://mysparks/data/raw/statcan/"
OUT_PATH = "s3://mysparks/OUTPUT-Folder/"
SCHEMA_PATH = "schema/statcan/"
# IN_PATH = "../data/clean/statcan/"
# OUT_PATH = "../OUTPUT-Folder/"
# SCHEMA_PATH = "../schema/statcan/"
cc_id = "13100781"

s3_obj = boto3.client('s3')
s3_cc_obj = s3_obj.get_object(Bucket='mysparks', Key=SCHEMA_PATH + cc_id + ".json")
s3_cc_data = s3_cc_obj['Body'].read().decode('utf-8')
input_schema = json.loads(s3_cc_data)
# input_schema = json.load(open("../schema/statcan/" + cc_id + ".json"))

os.makedirs(OUT_PATH, exist_ok=True)


def boolean_interpreter(new_df, column_name: str) -> dataframe.DataFrame:
    new_df = new_df.withColumn(column_name + '_interpreted',
                               when(new_df[column_name] == 1, True)
                               .otherwise(when(new_df[column_name] == 2, False)
                                          .otherwise(None)))
    new_df = new_df.drop(column_name).withColumnRenamed(column_name + '_interpreted', column_name)
    return new_df


def main():
    raw_input = spark.read.csv(IN_PATH + cc_id + '/*.csv',
                               header=True,
                               schema=types.StructType.fromJson(input_schema))  # reading COVID-19 Data csv

    grouped_pivot = raw_input.groupBy(raw_input['Case identifier number']) \
        .pivot("Case information", ['Region', 'Gender', 'Age group', 'Occupation', 'Transmission', 'Hospital status',
                                    'Episode week', 'Recovery week', 'Asymptomatic', 'Recovered', 'Death']) \
        .agg(first(raw_input['VALUE']))  # ? Compare performance of first vs avg

    # region Episode week and Recovery week interpreted
    new_df = grouped_pivot.withColumn('Episode month',
                                      when(grouped_pivot['Episode week'].between(0, 4), "2020-01")
                                      .otherwise(when(grouped_pivot['Episode week'].between(5, 8), "2020-02")
                                          .otherwise(
                                          when(grouped_pivot['Episode week'].between(9, 12), "2020-03")
                                              .otherwise(
                                              when(grouped_pivot['Episode week'].between(13, 17), "2020-04")
                                                  .otherwise(
                                                  when(grouped_pivot['Episode week'].between(18, 21),
                                                       "2020-05")
                                                      .otherwise(
                                                      when(grouped_pivot['Episode week'].between(22, 25),
                                                           "2020-06")
                                                          .otherwise(
                                                          when(grouped_pivot['Episode week'].between(26, 30),
                                                               "2020-07")
                                                              .otherwise(
                                                              when(
                                                                  grouped_pivot['Episode week'].between(31, 34),
                                                                  "2020-08")
                                                                  .otherwise(
                                                                  when(
                                                                      grouped_pivot['Episode week'].between(35, 38),
                                                                      "2020-09")
                                                                      .otherwise(
                                                                      when(
                                                                          grouped_pivot['Episode week'].between(39, 43),
                                                                          "2020-10")
                                                                          .otherwise(when(
                                                                          grouped_pivot['Episode week'].between(44, 47),
                                                                          "2020-11")
                                                                          .otherwise(when(
                                                                          grouped_pivot['Episode week'].between(48, 52),
                                                                          "2020-12")
                                                                          .otherwise(
                                                                          None)))))))))))))

    new_df = new_df.withColumn('Recovery month',
                               when(new_df['Recovery week'].between(0, 4), "2020-01")
                               .otherwise(when(new_df['Recovery week'].between(5, 8), "2020-02")
                                   .otherwise(when(new_df['Recovery week'].between(9, 12), "2020-03")
                                   .otherwise(when(new_df['Recovery week'].between(13, 17), "2020-04")
                                   .otherwise(when(new_df['Recovery week'].between(18, 21), "2020-05")
                                   .otherwise(when(new_df['Recovery week'].between(22, 25), "2020-06")
                                   .otherwise(when(new_df['Recovery week'].between(26, 30), "2020-07")
                                   .otherwise(
                                   when(new_df['Recovery week'].between(31, 34), "2020-08")
                                       .otherwise(
                                       when(
                                           new_df['Recovery week'].between(35, 38),
                                           "2020-09")
                                           .otherwise(
                                           when(
                                               new_df['Recovery week'].between(39, 43),
                                               "2020-10")
                                               .otherwise(when(
                                               new_df['Recovery week'].between(44, 47),
                                               "2020-11")
                                               .otherwise(when(
                                               new_df['Recovery week'].between(48, 52),
                                               "2020-12")
                                               .otherwise(
                                               None)))))))))))))
    # endregion

    # region Region interpreted
    new_df = new_df.withColumn('Region_interpreted',
                               when(new_df['Region'] == 5, "British Columbia and Yukon")
                               .otherwise(
                                   when(new_df['Region'] == 4, "Prairies and the Northwest Territories")
                                       .otherwise(when(new_df['Region'] == 3, "Ontario and Nunavut")
                                       .otherwise(when(new_df['Region'] == 2, "Quebec")
                                       .otherwise(
                                       when(new_df['Region'] == 1, "Atlantic")
                                           .otherwise(None))))))
    new_df = new_df.drop('Region').withColumnRenamed('Region_interpreted', 'Region')
    # endregion

    # region Gender interpreted
    new_df = new_df.withColumn('Gender_interpreted',
                               when(new_df['Gender'] == 1, "Male")
                               .otherwise(when(new_df['Gender'] == 2, "Female")
                                          .otherwise(when(new_df['Gender'] == 9, "Not Stated")
                                                     .otherwise(None))))
    new_df = new_df.drop('Gender').withColumnRenamed('Gender_interpreted', 'Gender')
    # endregion

    # region Age group interpreted
    new_df = new_df \
        .withColumn('Age group_interpreted',
                    when(new_df['Age group'] == 1, "0-19")
                    .otherwise(when(new_df['Age group'] == 2, "20-29")
                        .otherwise(when(new_df['Age group'] == 3, "30-39")
                        .otherwise(when(new_df['Age group'] == 4, "40-49")
                        .otherwise(when(new_df['Age group'] == 5, "50-59")
                        .otherwise(
                        when(new_df['Age group'] == 6, "60-69")
                            .otherwise(when(new_df['Age group'] == 7, "70-79")
                                       .otherwise(when(new_df['Age group'] == 8, "80 <=")
                                                  .otherwise(when(new_df['Age group'] == 99, "Not Stated")
                                                             .otherwise(None))))))))))
    new_df = new_df.drop('Age group').withColumnRenamed('Age group_interpreted', 'Age_group')
    # endregion

    # region Occupation interpreted
    new_df = new_df.withColumn('Occupation_interpreted',
                               when(new_df['Occupation'] == 1, "Health care worker")
                               .otherwise(when(new_df['Occupation'] == 2, "School or daycare worker/attendee")
                                   .otherwise(
                                   when(new_df['Occupation'] == 3, "Long term care resident")
                                       .otherwise(when(new_df['Occupation'] == 4, "Other")
                                                  .otherwise(when(new_df['Occupation'] == 9, "Not stated")
                                                             .otherwise(None))))))
    new_df = new_df.drop('Occupation').withColumnRenamed('Occupation_interpreted', 'Occupation')
    # endregion

    # region Transmission interpreted
    new_df = new_df.withColumn('Transmission_interpreted',
                               when(new_df['Transmission'] == 1, "Domestic acquisition or Unknown source")
                               .otherwise(when(new_df['Transmission'] == 2, " International travel")
                                          .otherwise(when(new_df['Transmission'] == 9, "Not Stated")
                                                     .otherwise(None))))
    new_df = new_df.drop('Transmission').withColumnRenamed('Transmission_interpreted', 'Transmission')
    # endregion

    # region Hospital status interpreted
    new_df = new_df.withColumn('Hospital status_interpreted',
                               when(new_df['Hospital status'] == 1, "Hospitalized and in ICU")
                               .otherwise(when(new_df['Hospital status'] == 2, "Hospitalized, but not in ICU")
                                   .otherwise(when(new_df['Hospital status'] == 3, "Not hospitalized")
                                   .otherwise(
                                   when(new_df['Hospital status'] == 9, "Not Stated/Unknown")
                                       .otherwise(None)))))
    new_df = new_df.drop('Hospital status').withColumnRenamed('Hospital status_interpreted', 'Hospital status')
    # endregion

    # region Boolean Interpretations
    new_df = boolean_interpreter(new_df, "Asymptomatic")
    new_df = boolean_interpreter(new_df, "Recovered")
    new_df = boolean_interpreter(new_df, "Death")
    # endregion

    new_df.cache()
    with open("s3://mysparks/" + SCHEMA_PATH + "covid_cases.json", 'w') as out_file:
        out_file.write(new_df.schema.json())
    new_df.coalesce(40).write.csv(OUT_PATH + "covid_cases", header=True, mode='overwrite')

    # region Provincial covid data
    weekly_cases = new_df.groupby(new_df['Episode week'], new_df['Region'])
    international_transmission = new_df.filter(new_df['Transmission'].like("%International%travel%"))\
        .groupby(new_df['Episode week'], new_df['Region']).count() \
        .withColumnRenamed('count', 'International Transmissions').withColumnRenamed('Region', 'Area')
    asymptomatic_cases = new_df.filter(new_df['Asymptomatic']).groupby(new_df['Episode week'], new_df['Region'])\
        .count().withColumnRenamed('count', 'Asymptomatic cases').withColumnRenamed('Region', 'Area')
    recovered_weekly = new_df.filter(new_df['Recovered']).groupby(new_df['Recovery week'], new_df['Region'])
    deaths_weekly = new_df.filter(new_df['Death']).groupby(new_df['Episode week'], new_df['Region'])
    new_cases = weekly_cases.count() \
        .withColumnRenamed('Episode week', 'Week Number').withColumnRenamed('count', 'Total New cases')
    age_splits = weekly_cases.pivot('Age_group',
                                    ["0-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80 <=",
                                     "Not Stated"]) \
        .count().withColumnRenamed('Region', 'Area').withColumnRenamed('Episode week', 'Ep Week')
    recovered_cases = recovered_weekly.count().withColumnRenamed('count', 'Recovered cases')\
        .withColumnRenamed('Region', 'Area')
    deaths = deaths_weekly.count().withColumnRenamed('count', 'Deaths').withColumnRenamed('Region', 'Area') \
        .withColumnRenamed('Episode week', 'Week No')

    # Joining all properties required from covid_cases
    covid_provinces = new_cases.join(recovered_cases, (new_cases['Week Number'] == recovered_cases['Recovery week'])
                                     & (new_cases['Region'] == recovered_cases['Area']), 'left') \
        .drop(recovered_cases['Recovery week']).drop(recovered_cases['Area']) \
        .join(deaths, (new_cases['Week Number'] == deaths['Week No'])
              & (new_cases['Region'] == deaths['Area']), 'left').drop(deaths['Week No']).drop(deaths['Area']) \
        .join(age_splits, (new_cases['Week Number'] == age_splits['Ep Week'])
              & (new_cases['Region'] == age_splits['Area']), 'left')\
        .drop(age_splits['Ep Week']).drop(age_splits['Area']) \
        .join(international_transmission, (new_cases['Week Number'] == international_transmission['Episode week'])
              & (new_cases['Region'] == international_transmission['Area']), 'left') \
        .drop(international_transmission['Episode week']).drop(international_transmission['Area']) \
        .join(asymptomatic_cases, (new_cases['Week Number'] == asymptomatic_cases['Episode week'])
              & (new_cases['Region'] == asymptomatic_cases['Area']), 'left') \
        .drop(asymptomatic_cases['Episode week']).drop(asymptomatic_cases['Area'])
    # endregion

    with open("s3://mysparks/" + SCHEMA_PATH + "provincial_cases.json", 'w') as out_file:
        out_file.write(covid_provinces.schema.json())
    covid_provinces.orderBy('Week Number').coalesce(1).fillna(0)\
        .write.csv(OUT_PATH + "provincial_cases", header=True, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Covid-19 Cases').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
