from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, greatest, sum


_ORCAMENTOS = "gold.orcamentos"
_CATEGORIAS = "gold.categorias"

_SNAPSHOT = "_src_budget_balance"

_ANALYTICS = "analytics.budget_balance"


@dp.materialized_view(name=_SNAPSHOT, private=True)
def _source():
    cat = spark.read.table(_CATEGORIAS).select("agrupador", "categoria", "subcategoria")

    orc = (
        spark.read.table(_ORCAMENTOS)
        .join(cat, ["categoria", "subcategoria"], "left")
        .select(
            "data_orcamento",
            "agrupador",
            "categoria",
            "subcategoria",
            col("planejado").alias("orcado_subcategoria"),
            col("consumido").alias("real_subcategoria"),
            "saldo",
        )
        .withColumn(
            "orcado_agrupador", sum("orcado_subcategoria").over(Window.partitionBy(["data_orcamento", "agrupador"]))
        )
        .withColumn(
            "orcado_categoria", sum("orcado_subcategoria").over(Window.partitionBy(["data_orcamento", "categoria"]))
        )
        .withColumn(
            "real_agrupador", sum("real_subcategoria").over(Window.partitionBy(["data_orcamento", "agrupador"]))
        )
        .withColumn(
            "real_categoria", sum("real_subcategoria").over(Window.partitionBy(["data_orcamento", "categoria"]))
        )
        .withColumn("saldo_orcamento", greatest(lit(0), col("orcado_subcategoria") - col("real_subcategoria")))
        .select(
            "data_orcamento",
            "agrupador",
            "categoria",
            "subcategoria",
            "orcado_agrupador",
            "orcado_categoria",
            "orcado_subcategoria",
            "real_agrupador",
            "real_categoria",
            "real_subcategoria",
            "saldo_orcamento",
        )
    )

    return orc


dp.create_streaming_table(name=_ANALYTICS)

dp.create_auto_cdc_from_snapshot_flow(
    target=_ANALYTICS,
    source=_SNAPSHOT,
    keys=["data_orcamento", "agrupador", "categoria", "subcategoria"],
    stored_as_scd_type=1,
)
