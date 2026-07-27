from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, lit


_SOURCE = "_src_gold_categorias"
_SILVER = "silver.categorias"
_GOLD = "gold.categorias"

_ESTILO_DE_VIDA = ["Habitação", "Saúde", "Educação", "Transporte", "Outros Fixos"]


@dp.temporary_view(name=_SOURCE)
def _source():
    df_gold = (
        spark.readStream.table(_SILVER)
        .withColumn(
            "agrupador",
            when(col("categoriaPai") == "Rendimentos", lit("Rendimentos"))
            .when(col("categoriaPai").isin(_ESTILO_DE_VIDA), lit("Estilo de Vida"))
            .otherwise(col("categoriaPai")),
        )
        .select(
            "id",
            col("categoriaPai").alias("categoria"),
            col("categoria").alias("subcategoria"),
            "natureza",
            "agrupador",
            "_ingesttime",
        )
    )
    return df_gold


dp.create_streaming_table(name=_GOLD)

dp.create_auto_cdc_flow(target=_GOLD, source=_SOURCE, keys=["id"], sequence_by="_ingesttime", stored_as_scd_type=1)
