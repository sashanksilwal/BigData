# pyspark 
titanic_df = spark.read.csv("titanic.csv", header=True, inferSchema=True)
titanic_df.show(5)

# 1. Count passengers who survived.
titanic_df.filter(titanic_df.Survived == 1).count()

# Retrieve 10 passengers who survived.
titanic_df.filter(titanic_df.Survived == 1).limit(10).show()

# 2. Add a column called "IsChild"
from pyspark.sql.functions import when
titanic_df = titanic_df.withColumn("IsChild", when(titanic_df.Age < 18, True).otherwise(False))
titanic_df.show(5)
titanic_df.filter(titanic_df.IsChild == True).count()

# 3. Group the DataFrame by the "Pclass" (Ticket class) column and count the number of passengers in each class.
from pyspark.sql.functions import count
titanic_df.groupBy("Pclass").agg(count("PassengerId").alias("PassengerCount")).show()

# 4. rename the "Pclass" column to "PassengerClass" 
titanic_df = titanic_df.withColumnRenamed("Pclass", "PassengerClass")

# 5. sort the DataFrame by the "Age" column in descending order
titanic_df = titanic_df.sort("Age", ascending=False)

# 6. Calculate the average age of passengers in each passenger class using the groupBy and agg functions.
from pyspark.sql.functions import avg
titanic_df.groupBy("PassengerClass").agg(avg("Age").alias("AverageAge")).show()

# 7. top 5 passengers with the highest fare
titanic_df.sort("Fare", ascending=False).limit(5).show()

# drop null values in the "Embarked" column
titanic_df = titanic_df.dropna(subset=["Embarked"])

# 8. Create a new DataFrame that contains the total fare collected per embarkation point (C = Cherbourg, 
# Q = Queenstown, S = Southampton). Then, join this DataFrame with the Titanic dataset on the "Embarked" 
# column
fare_by_embarked_df = titanic_df.groupBy("Embarked").agg({"Fare": "sum"}).withColumnRenamed("sum(Fare)", "Total_Fare_per_Embarked")
fare_by_embarked_df.show()
titanic_df = titanic_df.join(fare_by_embarked_df, "Embarked")
titanic_df.show(5)

# 9. Calculate the survival rate per class and gender.
titanic_df.groupBy(["PassengerClass", "Sex"]).agg(avg("Survived").alias("SurvivalRate")).show()

# 10. Save the modified DataFrame titanic_df to parquet format and read it back
titanic_df.write.parquet("titanic.parquet")
titanic_df_parquet = spark.read.parquet("titanic.parquet")
titanic_df_parquet.show(5)


