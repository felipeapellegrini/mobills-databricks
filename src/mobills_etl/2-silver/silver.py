from pyspark import pipelines as dp
from pyspark.sql.functions import *


SOURCES = [
    {"slv": {"table_name": "cartoes", "natural_keys": ["id"]}},
    {"slv": {"table_name": "categorias", "natural_keys": ["id"]}},
    {"slv": {"table_name": "orcamentos", "natural_keys": ["subcategoria", "categoria", "_businessdate"]}},
    {"slv": {"table_name": "contas", "natural_keys": ["id"]}},
    {"slv": {"table_name": "transacoes", "natural_keys": ["id", "conta_id", "_businessdate"]}},
]


def make_silver(slv):
    table_name = slv["table_name"]
    natural_keys = slv["natural_keys"]

    src_table = f"bronze.{table_name}"
    tgt_table = f"silver.{table_name}"

    dp.create_streaming_table(
        name=tgt_table, comment=f"Silver {table_name} - deduplicated by {natural_keys} SCD type 1"
    )

    dp.create_auto_cdc_flow(target=tgt_table, source=src_table, keys=natural_keys, sequence_by="_ingesttime")

    return tgt_table


silver_tables = [make_silver(**silver) for silver in SOURCES]
