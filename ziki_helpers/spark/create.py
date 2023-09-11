from pyspark.sql import SparkSession


def get_spark_for_delta_s3(aws_region='us-east-1'):
    spark_jars_packages = (
        "com.amazonaws:aws-java-sdk:1.12.246,"
        "org.apache.hadoop:hadoop-aws:3.2.2,"
        "io.delta:delta-core_2.12:2.4.0"
    )

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("PySparkLocal")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.hadoop.fs.s3a.connection.timeout", "3600000")
        .config("spark.hadoop.fs.s3a.connection.maximum", "1000")
        .config("spark.hadoop.fs.s3a.threads.max", "1000")
        .config("spark.jars.packages", spark_jars_packages)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
        #.config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .getOrCreate()
    )
    return spark