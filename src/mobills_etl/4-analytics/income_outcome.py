from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, when, lit

_TRANSACOES = "gold.transacoes"
_SNAPSHOT = "_src_income_outcome"

_ANALYTICS = "analytics.income_vs_outcome"


@dp.materialized_view(name=_SNAPSHOT, private=True)
def _source():
    return (
        spark.read.table(_TRANSACOES)
        .filter((col("ano_mes") >= "2026-01-01") & (col("natureza").isin(["Despesas", "Receitas"])))
        .groupBy(["ano_mes", "natureza"])
        .agg(sum(col("valor_abs")).alias("valor_abs"))
        .withColumn("despesas", when(col("natureza") == "Despesas", col("valor_abs")).otherwise(lit(0)))
        .withColumn("receitas", when(col("natureza") == "Receitas", col("valor_abs")).otherwise(lit(0)))
        .select("ano_mes", "despesas", "receitas")
    )


dp.create_streaming_table(name=_ANALYTICS)

dp.create_auto_cdc_from_snapshot_flow(target=_ANALYTICS, source=_SNAPSHOT, keys=["ano_mes"], stored_as_scd_type=1)
