from pyspark import pipelines as dp
from pyspark.sql.functions import regexp_extract, col, nullif, lit, date_format


_CONTAS = "silver.contas"
_CARTOES = "silver.cartoes"
_CATEGORIAS = "gold.categorias"
_TRANSACOES_PENDENTES = "silver.transacoes_pendentes"
_TRANSACOES_EFETIVADAS = "silver.transacoes_efetivadas"

_GOLD = "gold.transacoes"

_REGEX = r"\((\d+)/(\d+)\)"


@dp.materialized_view(name=_GOLD)
def _snapshot():
    contas = spark.read.table(_CONTAS)
    cartoes = spark.read.table(_CARTOES)
    categorias = spark.read.table(_CATEGORIAS)
    trs_pen = spark.read.table(_TRANSACOES_PENDENTES)
    trs_efe = spark.read.table(_TRANSACOES_EFETIVADAS)

    transacoes = trs_efe.unionByName(trs_pen)

    trs_gold = (
        transacoes.alias("trs")
        .join(contas.alias("ct"), on=(col("trs.conta_id") == col("ct.id")))
        .join(cartoes.alias("crt"), on=(col("trs.cartao_id") == col("crt.id")), how="left")
        .join(categorias.alias("cat"), on=(col("trs.tipo_transacao_filho_id") == col("cat.id")), how="left")
        .withColumn("parcela_atual", nullif(regexp_extract("trs.descricao", _REGEX, 1), lit("")).cast("int"))
        .withColumn("total_parcelas", nullif(regexp_extract("trs.descricao", _REGEX, 2), lit("")).cast("int"))
        .withColumn("futuro_vendido", (col("total_parcelas") - col("parcela_atual")) * col("valor_abs"))
        .withColumn("ano_mes", date_format(col("trs.data"), "yyyy-MM-01"))
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
            "cat.categoria",
            "cat.subcategoria",
            "cat.agrupador",
            "trs.valor",
            "trs.valor_abs",
            "parcela_atual",
            "total_parcelas",
            "futuro_vendido",
            "ano_mes",
        )
    )

    return trs_gold
