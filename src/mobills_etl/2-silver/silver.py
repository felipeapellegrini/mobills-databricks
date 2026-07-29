from pyspark import pipelines as dp


SOURCES = [
    {"slv": {"table_name": "cartoes", "natural_keys": ["id"], "load_strategy": "cdc"}},
    {"slv": {"table_name": "categorias", "natural_keys": ["id"], "load_strategy": "cdc"}},
    {
        "slv": {
            "table_name": "orcamentos",
            "natural_keys": ["subcategoria", "categoria", "_businessdate"],
            "load_strategy": "cdc",
        }
    },
    {"slv": {"table_name": "contas", "natural_keys": ["id"], "load_strategy": "cdc"}},
    {
        "slv": {
            "table_name": "transacoes_pendentes",
            "natural_keys": ["id", "tipo", "_businessmonth"],
            "load_strategy": "snapshot",
        }
    },
    {
        "slv": {
            "table_name": "transacoes_efetivadas",
            "natural_keys": ["id", "tipo", "_businessmonth"],
            "load_strategy": "cdc",
        }
    },
]


def register_cdc(table_name, natural_keys):
    src_table = f"bronze.{table_name}"
    tgt_table = f"silver.{table_name}"

    dp.create_streaming_table(
        name=tgt_table, comment=f"Silver {table_name} - deduplicated by {natural_keys} SCD type 1"
    )
    dp.create_auto_cdc_flow(target=tgt_table, source=src_table, keys=natural_keys, sequence_by="_ingesttime")


def register_snapshot(table_name):
    src_table = f"bronze.{table_name}"
    tgt_table = f"silver.{table_name}"

    @dp.materialized_view(name=tgt_table, comment=f"Silver {table_name} full refresh as MV")
    def _snapshot():
        bronze = spark.read.table(src_table)
        max_snapshot = bronze.selectExpr("max(_ingesttime) as _maxdt")

        return bronze.join(max_snapshot, bronze._ingesttime == max_snapshot._maxdt).drop("_maxdt")


def make_silver(slv):
    table_name = slv["table_name"]
    natural_keys = slv["natural_keys"]
    load_strategy = slv["load_strategy"]

    if load_strategy == "cdc":
        register_cdc(table_name, natural_keys)
    elif load_strategy == "snapshot":
        register_snapshot(table_name)


silver_tables = [make_silver(**silver) for silver in SOURCES]
