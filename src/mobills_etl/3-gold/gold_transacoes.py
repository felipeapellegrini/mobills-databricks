from pyspark import pipelines as dp
from pyspark.sql.functions import regexp_extract, col, nullif, lit


_CONTAS = "silver.contas"
_CARTOES = "silver.cartoes"
_CATEGORIAS = "silver.categorias"
_TRANSACOES = "silver.transacoes"

_SNAPSHOT = "_gold_transacoes"
_GOLD = "gold.transacoes"

_REGEX = r"\((\d+)/(\d+)\)"


@dp.materialized_view(name=_SNAPSHOT, private=True)
def _snapshot():
    contas = spark.read.table(_CONTAS)
    cartoes = spark.read.table(_CARTOES)
    categorias = spark.read.table(_CATEGORIAS)
    transacoes = spark.read.table(_TRANSACOES)

    trs_gold = (
        transacoes.alias("trs")
        .join(contas.alias("ct"), on=(col("trs.conta_id") == col("ct.id")))
        .join(cartoes.alias("crt"), on=(col("trs.cartao_id") == col("crt.id")), how="left")
        .join(categorias.alias("cat"), on=(col("trs.tipo_transacao_filho_id") == col("cat.id")), how="left")
        .withColumn("parcela_atual", nullif(regexp_extract("trs.descricao", _REGEX, 1), lit("")).cast("int"))
        .withColumn("total_parcelas", nullif(regexp_extract("trs.descricao", _REGEX, 2), lit("")).cast("int"))
        .withColumn("futuro_vendido", (col("total_parcelas") - col("parcela_atual")) * col("valor_abs"))
        .select(
            "trs.unique_id",
            col("trs.id").alias("id"),
            col("trs.data").alias("data_transacao"),
            "trs.descricao",
            "trs.tipo",
            "trs.situacao",
            col("ct.nome").alias("conta"),
            col("crt.nome").alias("cartao"),
            "cat.natureza",
            col("cat.categoriaPai").alias("categoria"),
            col("cat.categoria").alias("subcategoria"),
            "trs.valor",
            "trs.valor_abs",
            "parcela_atual",
            "total_parcelas",
            "futuro_vendido",
        )
    )

    return trs_gold


dp.create_streaming_table(_GOLD)

dp.create_auto_cdc_from_snapshot_flow(
    target=_GOLD, source=_SNAPSHOT, keys=["id", "data_transacao", "conta"], stored_as_scd_type=1
)
