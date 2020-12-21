import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import pyspark.sql.functions as f


df=spark.read.option("header","true").option("delimiter","|").csv("LOANS.TXT")


data = df.selectExpr("year(date_key) AS year","bid").groupBy("year").count()

finaldata  = data.sort(f.col("year"),f.col("count"))

finaldata  = data.withColumnRenamed("count","loans")

finaldata.repartition(1).write.format("csv").save("finaldata.csv",header='true')

spark.read.csv('finaldata.csv', header=True).coalesce(1).orderBy('year').toPandas().to_csv('loansdata.csv', index=False)

df=pd.read_csv("loansdata.csv")

df.plot(x='year', y='loans', kind='bar')

plt.savefig("loansPERyear.png")
