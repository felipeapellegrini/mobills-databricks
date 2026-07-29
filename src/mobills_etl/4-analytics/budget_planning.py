from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, coalesce, sum, lag
from pyspark.sql.window import Window

_TRANSACOES = "gold.transacoes"
_ORCAMENTOS = "gold.orcamentos"
_CALENDARIO = "gold.calendar"
_CATEGORIAS = "gold.categorias"
_SNAPSHOT = "_src_budget_planning"
_ANALYTICS = "analytics.budget_planning"

_PARTITIONBY = ["agrupador", "categoria", "subcategoria"]


@dp.materialized_view(name=_SNAPSHOT, private=True)
def _source():
    w_trs_avg_3m = Window.partitionBy(_PARTITIONBY).orderBy("ano_mes").rowsBetween(-4, -1)
    w_trs_avg_6m = Window.partitionBy(_PARTITIONBY).orderBy("ano_mes").rowsBetween(-7, -1)
    w_trs_lag_1m = Window.partitionBy(_PARTITIONBY).orderBy("ano_mes")

    cat = (
        spark.read.table(_CATEGORIAS)
        .filter(col("natureza") == "Despesas")
        .select("agrupador", "categoria", "subcategoria")
    )

    cal = spark.read.table(_CALENDARIO).select("ano_mes").distinct()

    trs = (
        spark.read.table(_TRANSACOES)
        .groupBy(["ano_mes", "agrupador", "categoria", "subcategoria"])
        .agg(sum("valor_abs").alias("valor"))
        .select("ano_mes", "agrupador", "categoria", "subcategoria", col("valor").alias("valor_transacao"))
    )

    orc = spark.read.table(_ORCAMENTOS).select(
        col("data_orcamento").alias("ano_mes"),
        "categoria",
        "subcategoria",
        col(
            "planejado",
        ).alias("valor_orcamento"),
        col("saldo").alias("saldo_orcamento"),
    )

    final_ds = (
        cal.join(cat, how="cross")
        .orderBy("ano_mes", "agrupador", "categoria", "subcategoria")
        .join(trs, on=["ano_mes", "agrupador", "categoria", "subcategoria"], how="left")
        .join(orc, on=["ano_mes", "categoria", "subcategoria"], how="left")
        .withColumn("media_3_meses", sum("valor_transacao").over(w_trs_avg_3m) / 3)
        .withColumn("media_6_meses", sum("valor_transacao").over(w_trs_avg_6m) / 6)
        .withColumn("valor_mes_anterior", lag("valor_transacao").over(w_trs_lag_1m))
        .select(
            "ano_mes",
            "agrupador",
            "categoria",
            "subcategoria",
            coalesce("valor_transacao", lit(0)).alias("valor_mes_atual"),
            coalesce("valor_mes_anterior", lit(0)).alias("valor_mes_anterior"),
            coalesce("media_3_meses", lit(0)).alias("media_3_meses"),
            coalesce("media_6_meses", lit(0)).alias("media_6_meses"),
            coalesce("valor_orcamento", lit(0)).alias("valor_orcamento"),
            coalesce("saldo_orcamento", lit(0)).alias("saldo_orcamento"),
        )
    )

    return final_ds


dp.create_streaming_table(name=_ANALYTICS)

dp.create_auto_cdc_from_snapshot_flow(
    target=_ANALYTICS,
    source=_SNAPSHOT,
    keys=["ano_mes", "agrupador", "categoria", "subcategoria"],
    stored_as_scd_type=1,
)
