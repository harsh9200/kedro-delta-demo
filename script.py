from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from pathlib import Path

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

data_dir = Path.cwd() / "data"
dbutils.fs.cp(
    f"file://{data_dir.as_posix()}", "dbfs:/data", recurse=True
)

# make sure DBFS ls returns a similar result
dbutils.fs.ls("dbfs:/kedro-delta-demo/data/01_raw/")
# [FileInfo(path='dbfs:/iris-databricks/data/01_raw/.gitkeep', name='.gitkeep', size=0),
# FileInfo(path='dbfs:/iris-databricks/data/01_raw/iris.csv', name='iris.csv', size=3858)]