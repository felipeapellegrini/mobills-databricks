from pyspark import pipelines as dp
from pyspark.sql.functions import col, date_format, explode, sequence, expr, to_date, min, max

_TRANSACOES = "gold.transacoes"
_ORCAMENTOS = "gold.orcamentos"

_SNAPSHOT = "_src_calendar"

_ANALYTICS = "gold.calendar"


@dp.materialized_view(name=_SNAPSHOT, private=True)
def _source():
    trs = spark.read.table(_TRANSACOES).select(col("data_transacao").alias("calendar_date")).distinct()
    orc = spark.read.table(_ORCAMENTOS).select(col("data_orcamento").alias("calendar_date")).distinct()

    _all = trs.unionByName(orc).select("calendar_date").distinct()

    _all_dates = _all.select(
        to_date(date_format(min("calendar_date"), "yyyy-MM-01")).alias("min_date"),
        to_date(date_format(max("calendar_date"), "yyyy-12-31")).alias("max_date"),
    )

    _all_dates = _all_dates.select(
        explode(sequence(col("min_date"), col("max_date"), expr("INTERVAL 1 DAY"))).alias("sk_data")
    ).withColumn("ano_mes", date_format("sk_data", "yyyy-MM-01"))

    return _all_dates


dp.create_streaming_table(name=_ANALYTICS)

dp.create_auto_cdc_from_snapshot_flow(target=_ANALYTICS, source=_SNAPSHOT, keys=["sk_data"], stored_as_scd_type=1)
