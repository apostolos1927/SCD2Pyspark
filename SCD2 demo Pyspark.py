# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# COMMAND ----------

data = [
        (1,'James','driver',15,datetime.today() - timedelta(days=12)),
        (1,'James','teacher',18,datetime.today() - timedelta(days=10)),
        (1,'James','engineer',23,datetime.today() - timedelta(days=8)),
        (4,'Washington','architect',30,datetime.today() - timedelta(days=7)),
        (5,'Jefferson','CEO',67,datetime.today() - timedelta(days=6))
        ]


# COMMAND ----------

data = [(1,'James','developer',30,datetime.today() - timedelta(days=3)),
        (4,'Washington','barman',15,datetime.today() - timedelta(days=3)),
        (6,'Michael','teacher',18,datetime.today() - timedelta(days=9)),
        (7,'Robert','engineer',23,datetime.today() - timedelta(days=8)) 
        ]

# COMMAND ----------


class SCD2:
    #save variables
    def __init__(self,primary_keys,order_column,delete_condition):
        self.primary_keys=primary_keys
        self.order_column=order_column
        self.delete_condition=delete_condition
    # SCD2
    def _apply_scd2(self,
                    df: DataFrame,
                    primary_keys: list[str],
                    order_column: str,
                   delete_condition: str
    ) -> DataFrame:
       
        df = df.withColumn(
            "__versioned",
            F.struct(F.col(order_column).alias("DateTimeValidFrom")),
            ).withColumn(
                "__versioned",
                F.col("__versioned").withField(
                    "DateTimeValidTo",
                    F.lead(order_column).over(
                        Window.partitionBy(*primary_keys).orderBy(order_column)
                    )
                    - F.expr("INTERVAL 1 SECONDS"),
                ),
            ).withColumn(
                "__versioned",
                F.col("__versioned").withField(
                    "DeletedFlag",
                    F.when(
                    (F.expr(delete_condition)) & (F.col("__versioned.DateTimeValidTo").isNull()),
                    F.expr(delete_condition)
                   ).otherwise(F.lit(False))
                ),
            ).withColumn(
                "__versioned",
                F.col("__versioned").withField(
                    "DateTimeValidTo",
                     F.when(
                    (F.col("__versioned.DeletedFlag") == True) & (F.col("__versioned.DateTimeValidTo").isNull()),
                    datetime.now()
                   ).otherwise(F.col("__versioned.DateTimeValidTo"))
                ),
            )
        
        return df
    #Check if table exists
    def catch(self,target_table):
        try:
            if spark.read.table(target_table):
               return True
        except Exception as e:
            return False
    # execute SCD2    
    def execute(self, df: DataFrame) -> DataFrame:
        
        source_df = self._apply_scd2(
            df=df,
            primary_keys=self.primary_keys,
            order_column=self.order_column,
            delete_condition=self.delete_condition
        )
        

        source_df.printSchema()

        target_df = (
            spark.read.table("target_table")
            if self.catch("target_table")
            else 
            spark.createDataFrame(spark.sparkContext.emptyRDD(), source_df.schema)
        )
        
        unioned = (target_df.alias("t")
                  .unionByName(source_df, allowMissingColumns=True).select([F.col("*")])
                  )
        
        
        return self._apply_scd2(
            df=unioned,
            primary_keys=self.primary_keys,
            order_column=self.order_column,
            delete_condition=self.delete_condition
        )



# COMMAND ----------

# MAGIC %md
# MAGIC Main code

# COMMAND ----------

df = spark.createDataFrame(data=data, schema = ["id","name","occupation","age","date_column"])

scd2_object = SCD2(["id"],"date_column","id==5")

df_final = scd2_object.execute(df)

df_final.write.mode('overwrite').saveAsTable("target_table")

# COMMAND ----------

spark.read.table("target_table").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE target_table
