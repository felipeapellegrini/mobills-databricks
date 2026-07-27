from pyspark import pipelines as dp
from pyspark.sql.functions import *
from datetime import datetime

SOURCES = [
    {
        "src": {
            "table_name": "cartoes",
            "businessdate_col": None,
            "file_type": "csv",
            "schema_evolution_policy": "addNewColumns",
        }
    },
    {
        "src": {
            "table_name": "categorias",
            "businessdate_col": None,
            "file_type": "csv",
            "schema_evolution_policy": "addNewColumns",
        }
    },
    {
        "src": {
            "table_name": "orcamentos",
            "businessdate_col": "data",
            "file_type": "csv",
            "schema_evolution_policy": "addNewColumns",
        }
    },
    {
        "src": {
            "table_name": "contas",
            "businessdate_col": None,
            "file_type": "csv",
            "schema_evolution_policy": "addNewColumns",
        }
    },
    {
        "src": {
            "table_name": "transacoes",
            "businessdate_col": "data",
            "file_type": "csv",
            "schema_evolution_policy": "addNewColumns",
        }
    },
]


_DTLOAD = datetime.now().strftime("%Y%m%d")


def make_bronze(src):
    table_name = src["table_name"]
    businessdate_col = src["businessdate_col"]
    file_type = src["file_type"]
    schema_evolution_policy = src["schema_evolution_policy"]

    @dp.table(
        name=f"bronze.{table_name}",
        comment="Bronze ingestion with SDP from Mobills extraction CSV via Auto Loader",
    )
    def bronze_table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.inferSchema", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.format", file_type)
            .option("cloudFiles.schemaEvolutionMode", schema_evolution_policy)
            .option("header", "true")
            .load(f"/Volumes/mobills/default/raw_data/{table_name}")
            .withColumn("_processdate", lit(_DTLOAD))
            .withColumn(
                "_businessdate", date_format(col(businessdate_col), "yyyyMMdd") if businessdate_col else lit(_DTLOAD)
            )
            .withColumn("_ingesttime", current_timestamp())
            .withColumn("_sourcefile", col("_metadata.file_path"))
        )


for src in SOURCES:
    make_bronze(**src)
