from pyspark.sql import SparkSession, functions as F, types as T
import kagglehub

path = kagglehub.dataset_download("tanishqpratap/e-commerce-orders-dataset")

spark: SparkSession = (
    SparkSession.builder.master("local[*]")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "root")
    .config("spark.hadoop.fs.s3a.secret.key", "root123456")
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.warehouse.dir", "s3a://iceberg/warehouse/")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# generate raw data files

RAW_DATA_PATH = "s3a://iceberg/not_warehouse/ecommerce/orders"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{path}/ecommerce_orders_10k_updated.csv")
)

df = df.withColumn("order_date", F.date_format(F.col("order_date"), "yyyy-MM-dd"))

(
    df.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet(RAW_DATA_PATH)
)

# create table and use add_files

TABLE = "spark_catalog.ecommerce.orders"

spark.sql("CREATE DATABASE spark_catalog.ecommerce")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        order_id          BIGINT,
        user_id           BIGINT,
        product_id        BIGINT,
        category          STRING,
        price             DOUBLE,
        qty               INT,
        total_price       DOUBLE,
        order_date        STRING,
        country           STRING,
        customer_segment  STRING,
        order_date_ts     TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (order_date)
""")

spark.sql(f"""
    CALL spark_catalog.system.add_files(
        table => '{TABLE}',
        source_table => '`parquet`.`{RAW_DATA_PATH}`'
    )
""").show(truncate=False)

# rewrite data files incrementally to relocate data files to table location
# for the demo I will just execute rewrite_data_files on 1 partition

spark.sql(f"""
CALL spark_catalog.system.rewrite_data_files(
    table => '{TABLE}', 
    where => 'order_date = "2023-04-22"', 
    options => map('rewrite-all', 'true')
)
""").show(truncate=False)
