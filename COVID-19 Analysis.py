# Databricks notebook source
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import plotly.express as ex

# COMMAND ----------

df_UnitedKingdom= spark.read.csv('/FileStore/tables/UK_Devolved_Nations_COVID19_2021_Dataset-3.csv', header = True, inferSchema = True )
df_PopulationData = spark.read.csv('/FileStore/tables/Populations_for_UK_and_Devolved_Nations.csv', header = True, inferSchema = True )

# COMMAND ----------

df_UnitedKingdom = df_UnitedKingdom.na.drop()
df_UnitedKingdom.show()

# COMMAND ----------

df_UnitedKingdom = df_UnitedKingdom.withColumnRenamed("areaName", "Nation")
df_UnitedKingdom.show(5)

# COMMAND ----------

df_UnitedKingdom = df_PopulationData.join(df_UnitedKingdom, on = 'Nation', how = 'inner')
df_UnitedKingdom.show(5)

# COMMAND ----------

df_UnitedKingdom.na.drop()
df_UnitedKingdom = df_UnitedKingdom.drop("_c0")
df_UnitedKingdom.show(5)

# COMMAND ----------

## Total active cases
df_UnitedKingdom = df_UnitedKingdom.withColumn('ActiveCases', ((df_UnitedKingdom["MildCases"] + df_UnitedKingdom["HospitalisedCases"])-df_UnitedKingdom["CasesCured"]-df_UnitedKingdom["ConfirmedDeaths"]))
df_UnitedKingdom.show(10)
display(df_UnitedKingdom)

# COMMAND ----------

## Rearranging columns
df_UnitedKingdom = df_UnitedKingdom.select("date","Nation","Population","MildCases","HospitalisedCases","CasesCured","ConfirmedDeaths","ActiveCases")

# COMMAND ----------

df_UnitedKingdom.groupBy("Nation").sum("MildCases", "HospitalisedCases").show()

# COMMAND ----------

## Cases in Wales
df_wales = df_UnitedKingdom.filter(df_UnitedKingdom["Nation"]=="Wales")
df_wales.show(5)

# COMMAND ----------

## Cases in Scotland
df_scotland = df_UnitedKingdom.filter(df_UnitedKingdom["Nation"]=="Scotland")
df_scotland.show(5)

# COMMAND ----------

## Total cases in scotland
df_UnitedKingdom.filter((df_UnitedKingdom['Nation'] == "Scotland") & (df_UnitedKingdom['MildCases'] > 0) & (df_UnitedKingdom['HospitalisedCases'] > 0)).count()

# COMMAND ----------

## Total cases in Wales
df_UnitedKingdom.filter((df_UnitedKingdom['Nation'] == "Wales") & (df_UnitedKingdom['MildCases'] > 0) & (df_UnitedKingdom['HospitalisedCases'] > 0)).count()

# COMMAND ----------

df_uk_latest_cases=df_UnitedKingdom.groupBy("date").sum().sort("date",ascending=True)
df_uk_latest_cases.select('date','sum(MildCases)', 'sum(HospitalisedCases)').toPandas().style.background_gradient(cmap='Reds')

# COMMAND ----------

df_uk_latest_cases=df_UnitedKingdom.groupBy("Nation").sum().sort("sum(ConfirmedDeaths)", ascending=False)
df_uk_latest_cases.select('Nation','sum(MildCases)','sum(HospitalisedCases)','sum(CasesCured)','sum(ConfirmedDeaths)','sum(ActiveCases)').toPandas().style.background_gradient(cmap='Reds')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Death cases in all 4 regions

# COMMAND ----------

df_state_death_cases = df_UnitedKingdom.filter(df_UnitedKingdom["ConfirmedDeaths"]>0)
df_state_death_cases.sort(df_state_death_cases["ConfirmedDeaths"].desc())
display(df_state_death_cases)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Confirmed, Active, Death Cases over a period of 2 years

# COMMAND ----------

graph = df_UnitedKingdom.groupby('date').sum().toPandas()
graph = graph.melt(id_vars='date', value_vars=['sum(CasesCured)', 'sum(ConfirmedDeaths)', 'sum(ActiveCases)'],
         var_name='Case', value_name='No. of Cases')
graph.head()
fig=ex.area(graph, x='date', y='No. of Cases', color='Case', title = 'Cases over a period of 2 years', color_discrete_sequence=["red", "blue", "green"])
fig.show()

# COMMAND ----------

## 

# COMMAND ----------



# COMMAND ----------


