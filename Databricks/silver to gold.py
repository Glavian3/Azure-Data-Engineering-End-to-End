# Databricks notebook source
table_name = []
for i in dbutils.fs.ls('/mnt/formula1dll125/silver/SalesUT/'):
    table_name.append(i.name)
print(table_name)

# COMMAND ----------

for name in table_name:
    path = "/mnt/formula1dll125/silver/SalesUT/" + name
    print(path)
    df = spark.read.format('delta').load(path)

    column_names = df.columns

    for old_col_name in column_names:

        new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i-1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")

        df = df.withColumnRenamed(old_col_name, new_col_name)

    output_path = "/mnt/formula1dll125/gold/SalesUT/" + name + '/'
    df.write.format('delta').mode('overwrite').save(output_path)

# COMMAND ----------

df.limit(10).display()