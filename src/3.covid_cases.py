import json
import os

from pyspark.sql import SparkSession, functions, types, dataframe

IN_PATH = "../data/raw/statcan/"
OUT_PATH = "../data/processed/statcan/"
SCHEMA_PATH = "../schema/statcan/"
cc_id = "13100781"
os.makedirs(OUT_PATH, exist_ok=True)
input_schema = json.load(open(SCHEMA_PATH + cc_id + ".json"))


def boolean_interpreter(new_df, column_name: str) -> dataframe.DataFrame:
    new_df = new_df.withColumn(column_name + '_interpreted',
                               functions.when(new_df[column_name] == 1, True)
                               .otherwise(functions.when(new_df[column_name] == 2, False)
                                          .otherwise(None)))
    new_df = new_df.drop(column_name).withColumnRenamed(column_name + '_interpreted', column_name)
    return new_df


def main():
    raw_input = spark.read.csv(IN_PATH + cc_id + '.csv',
                               header=True,
                               schema=types.StructType.fromJson(input_schema))  # reading COVID-19 Data csv

    grouped_pivot = raw_input.groupBy(raw_input['Case identifier number']) \
        .pivot("Case information", ['Region', 'Gender', 'Age group', 'Occupation', 'Transmission', 'Hospital status',
                                    'Episode week', 'Recovery week', 'Asymptomatic', 'Recovered', 'Death']) \
        .agg(functions.first(raw_input['VALUE']))  # ? Compare performance of functions.first vs functions.avg

    # region Episode week and Recovery week interpreted
    new_df = grouped_pivot.withColumn('Episode month',
                                      functions.when(grouped_pivot['Episode week'].between(0, 4), "2020-01")
                                      .otherwise(functions.when(grouped_pivot['Episode week'].between(5, 8), "2020-02")
                                          .otherwise(
                                          functions.when(grouped_pivot['Episode week'].between(9, 12), "2020-03")
                                              .otherwise(
                                              functions.when(grouped_pivot['Episode week'].between(13, 17), "2020-04")
                                                  .otherwise(
                                                  functions.when(grouped_pivot['Episode week'].between(18, 21),
                                                                 "2020-05")
                                                      .otherwise(
                                                      functions.when(grouped_pivot['Episode week'].between(22, 25),
                                                                     "2020-06")
                                                          .otherwise(
                                                          functions.when(grouped_pivot['Episode week'].between(26, 30),
                                                                         "2020-07")
                                                              .otherwise(
                                                              functions.when(
                                                                  grouped_pivot['Episode week'].between(31, 34),
                                                                  "2020-08")
                                                                  .otherwise(
                                                                  functions.when(
                                                                      grouped_pivot['Episode week'].between(35, 38),
                                                                      "2020-09")
                                                                      .otherwise(
                                                                      functions.when(
                                                                          grouped_pivot['Episode week'].between(39, 43),
                                                                          "2020-10")
                                                                          .otherwise(functions.when(
                                                                          grouped_pivot['Episode week'].between(44, 47),
                                                                          "2020-11")
                                                                          .otherwise(functions.when(
                                                                          grouped_pivot['Episode week'].between(48, 52),
                                                                          "2020-12")
                                                                          .otherwise(
                                                                          None)))))))))))))

    new_df = new_df.withColumn('Recovery month',
                               functions.when(new_df['Recovery week'].between(0, 4), "2020-01")
                               .otherwise(functions.when(new_df['Recovery week'].between(5, 8), "2020-02")
                                   .otherwise(
                                   functions.when(new_df['Recovery week'].between(9, 12), "2020-03")
                                       .otherwise(
                                       functions.when(new_df['Recovery week'].between(13, 17), "2020-04")
                                           .otherwise(
                                           functions.when(new_df['Recovery week'].between(18, 21),
                                                          "2020-05")
                                               .otherwise(
                                               functions.when(new_df['Recovery week'].between(22, 25),
                                                              "2020-06")
                                                   .otherwise(
                                                   functions.when(new_df['Recovery week'].between(26, 30),
                                                                  "2020-07")
                                                       .otherwise(
                                                       functions.when(
                                                           new_df['Recovery week'].between(31, 34),
                                                           "2020-08")
                                                           .otherwise(
                                                           functions.when(
                                                               new_df['Recovery week'].between(35, 38),
                                                               "2020-09")
                                                               .otherwise(
                                                               functions.when(
                                                                   new_df['Recovery week'].between(39, 43),
                                                                   "2020-10")
                                                                   .otherwise(functions.when(
                                                                   new_df['Recovery week'].between(44, 47),
                                                                   "2020-11")
                                                                   .otherwise(functions.when(
                                                                   new_df['Recovery week'].between(48, 52),
                                                                   "2020-12")
                                                                   .otherwise(
                                                                   None)))))))))))))
    # endregion

    # region Region interpreted
    new_df = new_df.withColumn('Region_interpreted',
                               functions.when(new_df['Region'] == 5, "British Columbia and Yukon")
                               .otherwise(
                                   functions.when(new_df['Region'] == 4, "Prairies and the Northwest Territories")
                                       .otherwise(functions.when(new_df['Region'] == 3, "Ontario and Nunavut")
                                       .otherwise(functions.when(new_df['Region'] == 2, "Quebec")
                                       .otherwise(
                                       functions.when(new_df['Region'] == 1, "Atlantic")
                                           .otherwise(None))))))
    new_df = new_df.drop('Region').withColumnRenamed('Region_interpreted', 'Region')
    # endregion

    # region Gender interpreted
    new_df = new_df.withColumn('Gender_interpreted',
                               functions.when(new_df['Gender'] == 1, "Male")
                               .otherwise(functions.when(new_df['Gender'] == 2, "Female")
                                          .otherwise(functions.when(new_df['Gender'] == 9, "Not Stated")
                                                     .otherwise(None))))
    new_df = new_df.drop('Gender').withColumnRenamed('Gender_interpreted', 'Gender')
    # endregion

    # region Age group interpreted
    new_df = new_df \
        .withColumn('Age group_interpreted',
                    functions.when(new_df['Age group'] == 1, "0-19")
                    .otherwise(functions.when(new_df['Age group'] == 2, "20-29")
                        .otherwise(functions.when(new_df['Age group'] == 3, "30-39")
                        .otherwise(functions.when(new_df['Age group'] == 4, "40-49")
                        .otherwise(functions.when(new_df['Age group'] == 5, "50-59")
                        .otherwise(
                        functions.when(new_df['Age group'] == 6, "60-69")
                            .otherwise(functions.when(new_df['Age group'] == 7, "70-79")
                                       .otherwise(functions.when(new_df['Age group'] == 8, "80 <=")
                                                  .otherwise(functions.when(new_df['Age group'] == 99, "Not Stated")
                                                             .otherwise(None))))))))))
    new_df = new_df.drop('Age group').withColumnRenamed('Age group_interpreted', 'Age_group')
    # endregion

    # region Occupation interpreted
    new_df = new_df.withColumn('Occupation_interpreted',
                               functions.when(new_df['Occupation'] == 1, "Health care worker")
                               .otherwise(functions.when(new_df['Occupation'] == 2, "School or daycare worker/attendee")
                                   .otherwise(
                                   functions.when(new_df['Occupation'] == 3, "Long term care resident")
                                       .otherwise(functions.when(new_df['Occupation'] == 4, "Other")
                                                  .otherwise(functions.when(new_df['Occupation'] == 9, "Not stated")
                                                             .otherwise(None))))))
    new_df = new_df.drop('Occupation').withColumnRenamed('Occupation_interpreted', 'Occupation')
    # endregion

    # region Transmission interpreted
    new_df = new_df.withColumn('Transmission_interpreted',
                               functions.when(new_df['Transmission'] == 1, "Domestic acquisition or Unknown source")
                               .otherwise(functions.when(new_df['Transmission'] == 2, " International travel")
                                          .otherwise(functions.when(new_df['Transmission'] == 9, "Not Stated")
                                                     .otherwise(None))))
    new_df = new_df.drop('Transmission').withColumnRenamed('Transmission_interpreted', 'Transmission')
    # endregion

    # region Hospital status interpreted
    new_df = new_df.withColumn('Hospital status_interpreted',
                               functions.when(new_df['Hospital status'] == 1, "Hospitalized and in ICU")
                               .otherwise(functions.when(new_df['Hospital status'] == 2, "Hospitalized, but not in ICU")
                                   .otherwise(functions.when(new_df['Hospital status'] == 3, "Not hospitalized")
                                   .otherwise(
                                   functions.when(new_df['Hospital status'] == 9, "Not Stated/Unknown")
                                       .otherwise(None)))))
    new_df = new_df.drop('Hospital status').withColumnRenamed('Hospital status_interpreted', 'Hospital status')
    # endregion

    # region Boolean Interpretations
    new_df = boolean_interpreter(new_df, "Asymptomatic")
    new_df = boolean_interpreter(new_df, "Recovered")
    new_df = boolean_interpreter(new_df, "Death")
    # endregion

    with open(SCHEMA_PATH + "covid_cases.json", 'w') as out_file:
        out_file.write(new_df.schema.json())
    new_df.coalesce(40).write.csv(OUT_PATH + cc_id, header=True, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Covid-19 Cases').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
