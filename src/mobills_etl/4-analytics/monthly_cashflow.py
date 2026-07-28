from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, lit, coalesce
from pyspark.sql.window import Window

_TRANSACOES = "gold.transacoes"
_ORCAMENTOS = "gold.orcamentos"
_CALENDAR = "gold.calendar"

_SOURCE = "_src_cashflow"
_TARGET = "gold.monthly_cashflow"


@dp.materialized_view(name=_SOURCE)
def _source():
    trs = (
        spark.read.table(_TRANSACOES)
        .groupBy(["ano_mes"])
        .agg(
            sum("valor").alias("total_transacoes"),
        )
    )
    orc = (
        spark.read.table(_ORCAMENTOS)
        .groupBy("data_orcamento")
        .agg(
            sum("saldo").alias("total_orcamento"),
        )
    )

    cal = spark.read.table(_CALENDAR).select("ano_mes").distinct()

    window = Window.orderBy("ano_mes").rowsBetween(Window.unboundedPreceding, 0)
    cashflow = (
        cal.join(trs, cal.ano_mes == trs.ano_mes, "left")
        .join(orc, cal.ano_mes == orc.data_orcamento, "left")
        .select(
            cal.ano_mes,
            coalesce(trs.total_transacoes, lit(0)).alias("total_transacoes"),
            coalesce(orc.total_orcamento, lit(0)).alias("total_orcamento"),
        )
        .withColumn("total_mes", col("total_transacoes") - col("total_orcamento"))
        .withColumn("fluxo_de_caixa", sum("total_mes").over(window))
    )

    return cashflow


dp.create_streaming_table(name=_TARGET)

dp.create_auto_cdc_from_snapshot_flow(target=_TARGET, source=_SOURCE, keys=["ano_mes"], stored_as_scd_type=1)
