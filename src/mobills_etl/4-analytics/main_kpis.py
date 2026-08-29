from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, sum, coalesce, lit


_TRANSACOES = "gold.transacoes"
_ORCAMENTO = "gold.orcamentos"
_CALENDARIO = "gold.calendar"
_CATEGORIAS = "gold.categorias"

_SNAPSHOT = "_src_main_kpis"
_ANALYTICS = "analytics.main_kpis"


@dp.materialized_view(name=_SNAPSHOT, private=True)
def _source():
    cal = spark.table(_CALENDARIO).select("ano_mes").distinct()
    cat = spark.read.table(_CATEGORIAS)

    trs = (
        spark.read.table(_TRANSACOES)
        .groupBy("ano_mes")
        .agg(
            sum(when(col("subcategoria") == "Salario", col("valor_abs"))).alias("total_salario"),
            sum(when(col("subcategoria") == "Parcelados", col("valor_abs"))).alias("parcelas_mes_atual"),
            sum(when(col("subcategoria") == "Parcelados", col("futuro_vendido"))).alias("total_parcelas"),
            sum(when(col("agrupador") == "Estilo de Vida", col("valor_abs"))).alias("total_estilo_de_vida"),
            sum(when(col("natureza") == "Receitas", col("valor_abs"))).alias("total_receitas"),
            sum(when(col("natureza") == "Despesas", col("valor_abs"))).alias("total_despesas"),
            sum(when(col("subcategoria") == "Dividas Caras", col("valor_abs"))).alias("total_sangramento"),
        )
        .withColumn("prc_estilo_de_vida", col("total_estilo_de_vida") / col("total_salario"))
        .withColumn("prc_parcelas", col("parcelas_mes_atual") / col("total_salario"))
    )

    orc = (
        spark.read.table(_ORCAMENTO)
        .join(cat, ["categoria", "subcategoria"], "inner")
        .groupBy("data_orcamento", "agrupador")
        .agg(
            sum(when(col("agrupador") == "Estilo de Vida", col("saldo"))).alias("orcado_estilo_vida"),
            (sum("saldo").alias("total_orcamento")),
            sum(when(col("subcategoria") == "Dividas caras", col("saldo"))).alias("orcado_juros"),
        )
        .groupBy("data_orcamento")
        .agg(
            sum("orcado_estilo_vida").alias("orcado_estilo_vida"),
            sum("total_orcamento").alias("total_orcamento"),
            sum("orcado_juros").alias("orcado_juros"),
        )
    )

    main_kpi = (
        cal.alias("cal")
        .join(trs.alias("trs"), cal.ano_mes == trs.ano_mes, "left")
        .join(orc.alias("orc"), cal.ano_mes == orc.data_orcamento, "left")
        .withColumn("performance_mes", trs.total_receitas - trs.total_despesas - orc.total_orcamento)
        .withColumn(
            "prc_prj_estilo_vida",
            (col("total_estilo_de_vida") + coalesce(col("orcado_estilo_vida"), lit(0))) / col("total_salario"),
        )
        .withColumn(
            "prc_sangramento",
            (coalesce(col("total_sangramento"), lit(0)) + coalesce(col("orcado_juros"), lit(0))) / col("total_salario"),
        )
        .select(
            cal.ano_mes,
            trs.total_salario,
            trs.total_parcelas,
            trs.total_receitas,
            trs.total_despesas,
            trs.prc_estilo_de_vida,
            trs.prc_parcelas,
            orc.total_orcamento,
            "performance_mes",
            "prc_prj_estilo_vida",
            orc.orcado_juros,
            "prc_sangramento",
        )
    )

    return main_kpi


dp.create_streaming_table(name=_ANALYTICS)
dp.create_auto_cdc_from_snapshot_flow(source=_SNAPSHOT, target=_ANALYTICS, keys=["ano_mes"], stored_as_scd_type=1)
