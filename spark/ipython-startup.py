# Defines the %%sql cell magic the notebooks use: runs the cell through Spark
# SQL and displays the result as a table. Installed as an IPython startup file.
from IPython.core.magic import register_cell_magic
from pyspark.sql import SparkSession


@register_cell_magic
def sql(line, cell):
    return SparkSession.builder.getOrCreate().sql(cell).toPandas()


del sql
