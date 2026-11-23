# Installed as an IPython startup file, so it runs in every notebook kernel.
import os

from IPython.core.magic import register_cell_magic
from pyspark.sql import SparkSession

# /bin/notebook starts JupyterLab as the PySpark "driver Python", and kernels
# inherit that setting. Without this, `!spark-submit job.py` in a notebook
# would launch another JupyterLab instead of running the job with python3.
os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
os.environ.pop("PYSPARK_DRIVER_PYTHON_OPTS", None)


# %%sql cell magic: runs the cell through Spark SQL and displays the result
# as a table.
@register_cell_magic
def sql(line, cell):
    return SparkSession.builder.getOrCreate().sql(cell).toPandas()


del sql, os
