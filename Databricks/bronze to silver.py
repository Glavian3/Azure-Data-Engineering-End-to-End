# Databricks notebook source
table_path = []
for i in dbutils.fs.ls(f'/mnt/formula1dll125/bronze/'):
    for j in dbutils.fs.ls(i.path):
        table_path.append(j.path)
print(table_path)


# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,date_format,to_timestamp
from pyspark.sql.types import TimestampType,StringType

for path in table_path:
    df = spark.read.parquet(path)
    column = df.columns

    for col in column:
        if "Date" in col or "date" in col:

            df = df.withColumn(col,df[col].cast(StringType()))
            df = df.withColumn(col,date_format(df[col],"yyyy-MM-dd"))
            # df = df.withColumn(col,date_format(from_utc_timestamp(df[col],"UTC","yyyy-MM-dd")))
    
    output_path = f"/mnt/formula1dll125/silver/SalesUT/{path.split('/')[5]}"
    df.write.mode('overwrite').format('delta').save(output_path)