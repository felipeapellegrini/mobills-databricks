from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, coalesce, lit
from pyspark.sql.window import Window


_TRANSACOES = "gold.transacoes"
_ORCAMENTOS = "gold.orcamentos"
_CALENDAR = "gold.calendar"

_SOURCE = "_src_daily_cashflow"
_TARGET = "analytics.daily_cashflow"


@dp.materialized_view(name=_SOURCE, private=True)
def _source():
    trs = (
        spark.read.table(_TRANSACOES)
        .groupBy(["data_transacao"])
        .agg(
            sum("valor").alias("total_transacoes"),
        )
        .select(col("data_transacao").alias("sk_data"), "total_transacoes")
    )
    orc = (
        spark.read.table(_ORCAMENTOS)
        .groupBy("data_orcamento")
        .agg(
            sum("saldo").alias("total_orcamento"),
        )
        .select(col("data_orcamento").alias("sk_data"), "total_orcamento")
    )

    cal = spark.read.table(_CALENDAR).select("sk_data")

    window = Window.orderBy(cal.sk_data).rowsBetween(Window.unboundedPreceding, 0)
    cashflow = (
        cal.join(trs, cal.sk_data == trs.sk_data, "left")
        .join(orc, cal.sk_data == orc.sk_data, "left")
        .select(
            cal.sk_data,
            coalesce(trs.total_transacoes, lit(0)).alias("total_transacoes"),
            coalesce(orc.total_orcamento, lit(0)).alias("total_orcamento"),
        )
        .withColumn("total_dia", col("total_transacoes") - col("total_orcamento"))
        .withColumn("fluxo_de_caixa", sum("total_dia").over(window))
    )
    return cashflow


dp.create_streaming_table(name=_TARGET)

dp.create_auto_cdc_from_snapshot_flow(target=_TARGET, source=_SOURCE, keys=["sk_data"], stored_as_scd_type=1)
