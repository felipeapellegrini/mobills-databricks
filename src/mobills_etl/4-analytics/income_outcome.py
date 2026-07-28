from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum

_TRANSACOES = "gold.transacoes"
_SNAPSHOT = "_src_income_outcome"

_ANALYTICS = "gold.income_vs_outcome"


@dp.materialized_view(name=_SNAPSHOT, private=True)
def _source():
    return (
        spark.read.table(_TRANSACOES)
        .filter((col("ano_mes") >= "2026-01-01") & (col("natureza").isin(["Despesas", "Receitas"])))
        .groupBy(["ano_mes", "natureza"])
        .agg(sum(col("valor_abs")).alias("valor_abs"))
        .orderBy("ano_mes", "natureza")
    )


dp.create_streaming_table(name=_ANALYTICS)

dp.create_auto_cdc_from_snapshot_flow(
    target=_ANALYTICS, source=_SNAPSHOT, keys=["ano_mes", "natureza"], stored_as_scd_type=1
)
