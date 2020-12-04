import json
import os

from pyspark.sql import SparkSession, functions, types, dataframe

IN_PATH = "../data/raw/statcan/"
OUT_PATH = "../data/processed/statcan/"
INPUT_SCHEMA_PATH = "../schema/statcan/"
OUTPUT_SCHEMA_PATH = "../schema/processed/"
cc_id = "13100781"
os.makedirs(OUT_PATH, exist_ok=True)
os.makedirs(OUTPUT_SCHEMA_PATH, exist_ok=True)
input_schema = json.load(open(INPUT_SCHEMA_PATH + cc_id + ".json"))


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
        .pivot("Case information") \
        .agg(functions.first(raw_input['VALUE']))  # ? Compare performance of functions.first vs functions.avg

    year_columns = [col for col in grouped_pivot.columns if "year" in col.split()]
    new_df = grouped_pivot.drop(*year_columns)

    # region Region interpreted
    new_df = new_df.withColumn('Region_interpreted',
                               functions.when(new_df['Region'] == 5, "British Columbia and Yukon")
                               .otherwise(
                                   functions.when(new_df['Region'] == 4, "Prairies and the Northwest Territories")
                                   .otherwise(functions.when(new_df['Region'] == 3, "Ontario and Nunavut")
                                              .otherwise(functions.when(new_df['Region'] == 2, "Quebec")
                                                         .otherwise(functions.when(new_df['Region'] == 1, "Atlantic")
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

    symptom_columns = [col for col in grouped_pivot.columns if col.startswith("Symptom,")]

    # region Boolean Interpretations
    new_df = boolean_interpreter(new_df, "Asymptomatic")
    new_df = boolean_interpreter(new_df, "Recovered")
    new_df = boolean_interpreter(new_df, "Death")
    for col in symptom_columns:
        new_df = boolean_interpreter(new_df, col)
    # endregion

    # To-Do :new_df = new_df.withColumn('Symptoms_observed')
    new_df = new_df.drop(*symptom_columns)

    with open(OUTPUT_SCHEMA_PATH + cc_id + ".json", 'w') as out_file:
        out_file.write(new_df.schema.json())
    new_df.write.csv(OUT_PATH + cc_id, header=True, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Covid-19 Cases').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
