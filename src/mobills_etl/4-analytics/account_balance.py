from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, sum
from pyspark.sql.window import Window


_TRANSACOES = "gold.transacoes"
_SNAPSHOT = "_src_account_balance"

_ANALYTICS = "analytics.account_balance"


@dp.materialized_view(name=_SNAPSHOT, private=True)
def _source():
    cta_window = Window.partitionBy("conta").orderBy("ano_mes")

    return (
        spark.read.table(_TRANSACOES)
        .filter(col("situacao") == "efetivado")
        .groupBy(["ano_mes", "conta", "cartao"])
        .agg(sum(col("valor")).alias("resultado_mes"))
        .withColumn("saldo_conta", sum("resultado_mes").over(cta_window))
        .withColumn("saldo_cartao", when(col("cartao").isNotNull(), col("resultado_mes")))
    )


dp.create_streaming_table(name=_ANALYTICS)

dp.create_auto_cdc_from_snapshot_flow(
    target=_ANALYTICS, source=_SNAPSHOT, keys=["ano_mes", "conta", "cartao"], stored_as_scd_type=1
)
