package top.aniss.spark.chapter_6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataTypes {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local[4]").appName("DataTypes").getOrCreate()
        //booleanExpr(spark)
        //numbers(spark)
        //strings(spark)
        //RegularExpr(spark)
        Dates(spark)

        spark.close()
    }

    def Dates(spark: SparkSession) = {

        val df = spark.read.option("header", "true")
            .csv("/home/andy/IdeaProjects/SparkDefinitiveGuide/data/retail-data/by-day/2010-12-01.csv")
        df.printSchema()
        df.show(3, false)

        val dateDF = spark.range(1000)
            .withColumn("today", current_date())
            .withColumn("now", current_timestamp())
        dateDF.show(1000, false)
        dateDF.createOrReplaceTempView("dateTable")
        dateDF.printSchema()

    }

    def RegularExpr(spark: SparkSession): Unit = {
        import spark.implicits._

        val df = spark.read.option("header", "true")
            .csv("/home/andy/IdeaProjects/SparkDefinitiveGuide/data/retail-data/by-day/2010-12-01.csv")

        val simpleColors = Seq("black", "white", "red", "green", "blue")
        val regexString = simpleColors.map(_.toUpperCase()).mkString("|")

        println(regexString) // BLACK|WHITE|RED|GREEN|BLUE
        // 如果遇到BLACK|WHITE|RED|GREEN|BLUE中的任意一个, 则替换为COLOR
        df.select(regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"), $"Description").show(2, false)

        // 将LEET -> 1337 : L-1, E-3, T-7
        df.select(translate($"Description", "LEET", "1337"), $"Description").show(3, false)

        // 注意了解mkString的用法
        val regexString2 = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
        println(regexString2)

        // 抽取第一个颜色
        df.select(
            regexp_extract(col("Description"), regexString2, 1).alias("color_clean"),
            col("Description")).show(false)

        val containsBlack = col("Description").contains("BLACK")
        val containsWhite = col("DESCRIPTION").contains("WHITE")
        df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
            .where("hasSimpleColor")
            .select("Description").show(false)


        //  TODO  ......不太理解
        val simpleColors1 = Seq("black", "white", "red", "green", "blue")
        val selectedColumns = simpleColors1.map(color => {
            col("Description").contains(color.toUpperCase).alias(s"is_$color")
        }) :+ expr("*") // could also append this value
        println(selectedColumns)
        df.select(selectedColumns: _*).where(col("is_white").or(col("is_red")))
            .select("Description").show(false)
    }

    def strings(spark: SparkSession): Unit = {
        import spark.implicits._

        val df = spark.read.option("header", "true")
            .csv("/home/andy/IdeaProjects/SparkDefinitiveGuide/data/retail-data/by-day/2010-12-01.csv")

        // 每个word的首字母大写, 后面的字母都小写
        df.select(initcap($"Description"), initcap(lit("HELLO"))).show(2, false)

        // lower or upper
        df.select($"Description", lower($"Description"), upper(lower($"Description"))).show(2, false)

        // 去除空格, 和 pad ??
        df.select(
            ltrim(lit("    HELLO    ")).as("ltrim"),
            rtrim(lit("    HELLO    ")).as("rtrim"),
            trim(lit("    HELLO    ")).as("trim"),
            lpad(lit("HELLO"), 2, " ").as("lp2"),
            lpad(lit("HELLO"), 3, " ").as("lp3"),
            lpad(lit("HELLO"), 5, " ").as("lp5"),
            lpad(lit("HELLO"), 7, " ").as("lp7"),
            rpad(lit("HELLO"), 2, " ").as("rp2"),
            rpad(lit("HELLO"), 3, " ").as("rp3"),
            rpad(lit("HELLO"), 5, " ").as("rp5"),
            rpad(lit("HELLO"), 7, " ").as("rp7"),
            rpad(lit("HELLO"), 10, " ").as("rp10")
        ).show(2)
    }


    private def numbers(spark: SparkSession): Unit = {
        val df = spark.read.option("header", "true")
            .csv("/home/andy/IdeaProjects/SparkDefinitiveGuide/data/retail-data/by-day/2010-12-01.csv")
        val fabricatedQuality = pow(col("Quantity") * col("UnitPrice"), 2) + 5
        df.select(expr("CustomerID"), fabricatedQuality.alias("realQuantity")).show(2, false)

        df.selectExpr("CustomerID", "(POWER((Quantity * UnitPrice), 2) +5) as realQuantity")
            .show(2, false)

        df.select(round(col("UnitPrice"), 0)
            .alias("rounded"), col("UnitPrice")).show(5)

        //2.5 默认round成3, 如果想round成2, 使用bround
        df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

        //Error in 2.1.0
        //df.stat.corr("Quantity", "UnitPrice")

        // 计算相关性
        df.select(corr("Quantity", "UnitPrice")).show()

        df.describe().show()
        df.describe().show()
        df.describe().show()

        //val quantileProbs = Array(0.5)
        //val relError = 0.05
        //df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51
        df.stat.crosstab("StockCode", "Quantity").show()

        df.stat.freqItems(Seq("StockCode", "Quantity")).show(false)

        df.select(monotonically_increasing_id()).show(2)
    }

    private def booleanExpr(spark: SparkSession): Unit = {
        val df = spark.read.option("header", "true")
            .csv("/home/andy/IdeaProjects/SparkDefinitiveGuide/data/retail-data/by-day/2010-12-01.csv")
        df.printSchema()
        df.show()

        df.select(lit(5), lit("five"), lit(5.0)).show()

        df.where(col("InvoiceNo").equalTo(536365)).select("InvoiceNo", "Description").show(5, false)
        df.where(col("InvoiceNo") === 536365).select("InvoiceNo", "Description").show(5, false)
        df.where("InvoiceNo = 536365").show(5, false)
        df.where("InvoiceNo <> 536365").show(5, false) // Not equal


        val priceFilter = col("UnitPrice") > 600
        val descripFilter = col("Description").contains("POSTAGE")
        df.filter(col("StockCode").isin("DOT")).filter(priceFilter.or(descripFilter)).show(false)

        // Boolean expressions are not just reserved to filters. To filter a DataFrame, you can also just specify a Boolean column:
        val DOTCoderFilter = col("StockCode") === "DOT"
        df.withColumn("isExpensive", DOTCoderFilter.and(priceFilter.or(descripFilter)))
            .filter("isExpensive").select("UnitPrice", "isExpensive").show(false)


        df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
            .filter("isExpensive")
            .select("Description", "UnitPrice").show(false)

        // same above
        df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
            .filter("isExpensive")
            .select("Description", "UnitPrice").show(false)
        // same above
        df.withColumn("isExpensive", expr("UnitPrice > 250"))
            .filter("isExpensive")
            .select("Description", "UnitPrice").show(false)
    }
}
