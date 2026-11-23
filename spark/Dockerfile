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

# Spark ETL jobs. hadoop-aws is the S3A connector for reading raw files
# (s3a://bucket/...) that are not Iceberg tables; 3.3.4 matches the Hadoop
# client bundled with pyspark 3.5.2, and 1.12.262 is the AWS SDK it was built
# against. The PostgreSQL driver lets Spark read RDS tables over JDBC. Jars in
# $SPARK_HOME/jars load in notebooks and spark-submit alike, so no --jars.
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.6/postgresql-42.7.6.jar \
    -o ${SPARK_HOME}/jars/postgresql-42.7.6.jar

RUN echo '#! /bin/sh' >> /bin/notebook \
    && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-lab' >> /bin/notebook \
    && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --IdentityProvider.token='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
    && echo "pyspark" >> /bin/notebook \
    && chmod u+x /bin/notebook

COPY spark-defaults.conf log4j2.properties ${SPARK_HOME}/conf/
COPY .pyiceberg.yaml /root/.pyiceberg.yaml
COPY ipython-startup.py /root/.ipython/profile_default/startup/00-spark-sql.py

RUN mkdir -p /home/iceberg/notebooks

WORKDIR /home/iceberg
CMD ["/bin/notebook"]
