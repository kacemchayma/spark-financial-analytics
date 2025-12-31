import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.io.{File, PrintWriter}
import scala.util.Using

object StockPhase3 {

  def main(args: Array[String]): Unit = {

    val spark = StockUtils.getSparkSession(
      "Stock Analysis - Phase 3 Window Functions"
    )
    spark.sparkContext.setLogLevel("WARN")

    StockUtils.ensureOutputDir()

    try {

      val df = StockUtils.getDataFrame(spark)

      // =====================================================
      // 1. NETTOYAGE MINIMAL (requis pour fenêtres)
      // =====================================================
      val cleanDF = df
        .filter(col("bid_price1") > 0)
        .filter(col("ask_price1") > 0)
        .filter(col("stock_id").isNotNull)

      // =====================================================
      // 2. COLONNES DE BASE
      // =====================================================
      val baseDF = cleanDF
        .withColumn("spread", col("ask_price1") - col("bid_price1"))
        .withColumn(
          "mid_price",
          (col("ask_price1") + col("bid_price1")) / 2
        )

      // =====================================================
      // 3. DÉFINITION DES FENÊTRES
      // =====================================================
      val windowSpec =
        Window
          .partitionBy("stock_id", "time_id")
          .orderBy("seconds_in_bucket")
          .rowsBetween(-5, 0) // fenêtre glissante (6 points)

      val lagWindow =
        Window
          .partitionBy("stock_id", "time_id")
          .orderBy("seconds_in_bucket")

      // =====================================================
      // 4. ANALYSES TEMPORELLES
      // =====================================================
      val windowDF = baseDF
        // Moyennes mobiles
        .withColumn(
          "avg_spread_rolling",
          avg("spread").over(windowSpec)
        )
        .withColumn(
          "avg_mid_price_rolling",
          avg("mid_price").over(windowSpec)
        )

        // Variations temporelles
        .withColumn(
          "prev_mid_price",
          lag("mid_price", 1).over(lagWindow)
        )
        .withColumn(
          "price_change",
          col("mid_price") - col("prev_mid_price")
        )

        // Volatilité locale
        .withColumn(
          "volatility",
          stddev("mid_price").over(windowSpec)
        )

      // =====================================================
      // 5. AGRÉGATIONS RÉSUMÉES
      // =====================================================
      val summaryDF =
        windowDF
          .groupBy("stock_id")
          .agg(
            avg("avg_spread_rolling").alias("mean_spread"),
            avg("volatility").alias("mean_volatility"),
            avg(abs(col("price_change"))).alias("mean_price_change")
          )
          .orderBy(desc("mean_volatility"))
          .limit(10)

      // =====================================================
      // 6. ÉCRITURE DES RÉSULTATS
      // =====================================================
      Using(
        new PrintWriter(
          new File(s"${StockUtils.OUTPUT_DIR}/stock_phase3_results.txt")
        )
      ) { writer =>

        writer.println(
          "PHASE 3 – ANALYSE TEMPORELLE AVANCÉE (WINDOW FUNCTIONS)"
        )
        writer.println(
          "======================================================\n"
        )

        writer.println(
          "Fenêtres utilisées :"
        )
        writer.println(
          "- Fenêtre glissante de 6 observations (rowsBetween -5, 0)"
        )
        writer.println(
          "- Partitionnement par stock_id et time_id\n"
        )

        writer.println(
          "Top 10 stocks les plus volatils :"
        )

        summaryDF.collect().foreach { r =>
          writer.println(
            s"- stock_id=${r.get(0)} | " +
            f"spread_moyen=${r.getDouble(1)}%.6f | " +
            f"volatilité_moyenne=${r.getDouble(2)}%.6f | " +
            f"variation_prix=${r.getDouble(3)}%.6f"
          )
        }
      }

      println(
        s"Fichier ${StockUtils.OUTPUT_DIR}/stock_phase3_results.txt généré avec succès ✅"
      )

    } catch {
      case e: Exception =>
        println(s"❌ Erreur Phase 3 : ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }
}
