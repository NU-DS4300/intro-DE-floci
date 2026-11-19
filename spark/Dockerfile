# Debian 12 (bookworm). Debian 11 (bullseye) reached end of LTS in Aug 2026 and
# its security packages are no longer served from deb.debian.org.
FROM python:3.10-bookworm

# Spark only needs a headless JRE; the full JDK drags in X11/Mesa/ALSA.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    vim \
    unzip \
    openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# AWS CLI v2, so students can inspect the lake with the same commands they
# would run against real AWS (aws s3 ls, aws glue get-tables, aws athena ...).
RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o awscliv2.zip && \
    unzip -q awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Spark itself comes from the `pyspark` package in requirements.txt: the PyPI
# distribution bundles the full Spark runtime (jars, bin/, sbin/), and PyPI's
# CDN is far faster than the Apache mirrors.
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.9.2

# Iceberg runtime for Spark.
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar \
    -o ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Iceberg AWS bundle. Supplies S3FileIO, the AWS SDK v2, and the SigV4 request
# signer the Glue Iceberg REST endpoint requires. The original chapter did not
# need this because the iceberg-rest-fixture accepted unsigned requests.
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    -o ${SPARK_HOME}/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

RUN echo '#! /bin/sh' >> /bin/notebook \
    && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-lab' >> /bin/notebook \
    && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --IdentityProvider.token='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
    && echo "pyspark" >> /bin/notebook \
    && chmod u+x /bin/notebook

COPY spark-defaults.conf ${SPARK_HOME}/conf/
COPY .pyiceberg.yaml /root/.pyiceberg.yaml
COPY ipython-startup.py /root/.ipython/profile_default/startup/00-spark-sql.py

RUN mkdir -p /home/iceberg/notebooks

WORKDIR /home/iceberg
CMD ["/bin/notebook"]
