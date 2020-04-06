from pyspark.sql import SparkSession

class spark_contexts():
    __instance = None

    def __new__(self):
        if spark_contexts.__instance is None:
            spark_contexts.__instance = object.__new__(self)
            spark_contexts.spark = SparkSession.builder \
                .appName("RLE Encoder") \
                .enableHiveSupport().getOrCreate()
        else:
            print("object already created")
        return spark_contexts.__instance

    def get_spark(self):

        return spark_contexts.spark

    def get_spark_context(self):
        return self.get_spark().sparkContext