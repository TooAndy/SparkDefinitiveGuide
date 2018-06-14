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
        //Dates(spark)
        //nullException(spark)
        complexTypes(spark)

        spark.close()
    }

    def complexTypes(spark: SparkSession): Unit = {
        val df = spark.read.option("header", "true")
            .csv("/home/andy/IdeaProjects/SparkDefinitiveGuide/data/retail-data/by-day/2010-12-01.csv")
        df.printSchema()

        // struct
        df.selectExpr("(Description, InvoiceNo) as complex", "*").show(false)
        df.selectExpr("struct(Description, InvoiceNo) as complex", "*").show(false)

        val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"), col("*"))
        complexDF.show(false)

        complexDF.select("complex.Description").show(false)

        complexDF.select("complex.*").show(false)

        //Arrays
        val df_array = df.select(split(col("Description"), " ").alias("array_col"))
        df_array.show(2, false)
        df_array.selectExpr("array_col[0]").show(2, false)
        //Array Length
        df_array.select(col("array_col"), size(col("array_col"))).show(2, false)
        //array_contains
        df_array.select(col("array_col"), array_contains(col("array_col"), "WHITE"))
            .show(2, false)

        //explode - Creates a new row for each element in the given array or map column.
        val df_explode = df.withColumn("splitted", split(col("Description"), " "))
            .withColumn("exploded", explode(col("splitted")))
        df_explode.show(10, false)
        df_explode.select("Description", "splitted", "exploded").show(10, false)


        //Maps
    }

    def nullException(spark: SparkSession): Unit = {
        val df = spark.read.option("header", "true")
            .csv("/home/andy/IdeaProjects/SparkDefinitiveGuide/data/retail-data/by-day/2010-12-01.csv")
        df.printSchema()

        df.select(coalesce(col("Description"), col("CustomerId"))).show(false)

        df.createTempView("dfTable")
        spark.sql(
            """
              |SELECT
              |ifnull(null, 'return_value'),
              |nullif('value', 'value'),
              |nvl(null, 'return_value'),
              |nvl2('not null', 'return_value', 'else_value'),
              |nvl2(null, 'return_value', 'else_value')
              |FROM dfTable
            """.stripMargin).show(1)

        // Dropping rows containing any null values.
        df.na.drop().show(false)
        // any - 如果一行中有任意一个值为空, 就drop掉
        df.na.drop("any").show(10, false)
        // all - 一行中所有的值都为空, 才会drop掉
        df.na.drop("all").show(10, false)

        // If `how` is "all", then drop rows only if every `specified column` is null or NaN for that row.
        df.na.drop("all", Seq("StockCode", "InvoiceNo")).show(10, false)

        df.na.fill("All Null values become this string").filter(col("Description").contains("All"))
            .show(100, false)

        // fill specified column, other columns not to be filled
        df.na.fill("All Null values become this string", Seq("Description")).filter(col("Description").contains("All"))
            .show(100, false)

        // fill some columns with Map
        val fillColValues = Map("CustomerID" -> 5, "Description" -> "No Value")
        val df3 = df.na.fill(fillColValues).filter(col("Description").contains("No Value").or(col("CustomerID").equalTo(5)))
        df3.show(10, false)


        //replace : value should be the same type
        df3.na.replace("Description", Map("No Value" -> "UNKNOWN")).filter(col("Description").contains("UNKNOWN")).show(1000, false)


        // Ordering NUll
        // asc_nulls_first, desc_nulls_first, asc_nulls_last, or desc_nulls_last
        df.orderBy(asc_nulls_first("Description")).show(100, false)
        df.orderBy(asc_nulls_last("Description")).show(100, false)
        df.orderBy(desc_nulls_first("Description")).show(100, false)
        df.orderBy(desc_nulls_last("Description")).show(100, false)

    }


    def Dates(spark: SparkSession) = {
        import spark.implicits._

        val df = spark.read.option("header", "true")
            .csv("/home/andy/IdeaProjects/SparkDefinitiveGuide/data/retail-data/by-day/2010-12-01.csv")
        df.printSchema()

        //root
        //|-- InvoiceNo: string (nullable = true)
        //|-- StockCode: string (nullable = true)
        //|-- Description: string (nullable = true)
        //|-- Quantity: string (nullable = true)
        //|-- InvoiceDate: string (nullable = true)     // NOT timestamp
        //|-- UnitPrice: string (nullable = true)
        //|-- CustomerID: string (nullable = true)
        //|-- Country: string (nullable = true)


        df.show(3, false)

        val dateDF = spark.range(1000)
            .withColumn("today", current_date())
            .withColumn("now", current_timestamp())
        dateDF.show(3, false)
        dateDF.createOrReplaceTempView("dateTable")
        dateDF.printSchema()

        // 加减天数, 计算两个date相差的天数
        dateDF.select($"today",
            date_sub($"today", 5).alias("today-5"), // 减5天
            date_add($"today", 5).alias("today+5")) // 加5天
            .select($"today-5", $"today+5",
            datediff($"today+5", $"today-5").alias("diff") // 相减
        ).show(3)

        dateDF.withColumn("week_ago", date_sub($"today", 7))
            .select(datediff($"week_ago", $"today")).show(1)

        //The to_date function allows you to convert a string to a date
        val dateDF2 = dateDF.select(to_date(lit("2018-10-10")).alias("start"),
            to_date(lit("2019-06-19")).alias("end"))

        dateDF2.select(months_between($"end", $"start")).show(3)

        spark.range(1).withColumn("date", lit("2017-01-01"))
            .select(to_date(col("date"))).show()

        // Spark 不能解析year-day-month, 这种格式会返回null, 不会返回Exception
        // 如果year-day-month这种格式看起来和year-month-day一样, 则会解析错误.
        // 下面的时间是2016年12月20和2017年11月12日. 第一个解析成null, 第二个解析成了2017年12月11日, 解析出问题 - -
        dateDF.select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11"))).show(1)

        // 使用to_date和to_timestamp 解决上面的问题, Spark 2.2.0以后可用
        //val dateFormat = "yyyy-dd-MM"
        //dateDF.select(to_date(lit("2016-20-12"), dateFormat), to_date(lit("2017-12-11"), dateFormat)).show(1)
        //dateDF.withColumn("date", to_date(lit("2016-20-12"), dateFormat))
        //    .select($"date", to_utc_timestamp($"date", dateFormat)).show(false)

        dateDF2.filter($"start" > lit("2017-10-10")).show()
        dateDF2.filter($"start" > "'2017-12-12'").show()

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
