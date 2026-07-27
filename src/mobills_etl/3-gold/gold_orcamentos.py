from pyspark import pipelines as dp
from pyspark.sql.functions import round, lit, col, when
from datetime import datetime

_SILVER = "silver.orcamentos"
_GOLD = "gold.orcamentos"
_SNAPSHOT = "_src_gold_orcamentos"

_CUTOFF_DATE = datetime.now().strftime("%Y%m01")


@dp.materialized_view(name=_SNAPSHOT, private=True)
def _source():
    df_gold = (
        spark.read.table(_SILVER)
        .withColumn("consumido", round(col("efetivado") + col("previsto"), 2))
        .withColumn("base_saldo", round(col("planejado") - col("consumido"), 2))
        .withColumn(
            "saldo",
            when(col("_businessdate") < _CUTOFF_DATE, lit(0))
            .when(col("base_saldo") < 0, lit(0))
            .otherwise(col("base_saldo")),
        )
        .select(
            col("data").alias("data_orcamento"),
            "categoria",
            "subcategoria",
            "planejado",
            "consumido",
            "saldo",
            "_ingesttime",
        )
    )

    return df_gold


dp.create_streaming_table(name=_GOLD)

dp.create_auto_cdc_from_snapshot_flow(
    target=_GOLD, source=_SNAPSHOT, keys=["subcategoria", "categoria", "data_orcamento"], stored_as_scd_type=1
)
