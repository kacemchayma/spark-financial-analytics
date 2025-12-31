import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}
import scala.util.Using

object StockPhase2 {

  def main(args: Array[String]): Unit = {

    val spark =
      StockUtils.getSparkSession(
        "Stock Analysis - Phase 2 Descriptive Analysis"
      )
    spark.sparkContext.setLogLevel("WARN")

    StockUtils.ensureOutputDir()

    try {

      // =====================================================
      // 1. LECTURE + ÉCHANTILLONNAGE (COHÉRENT PHASE 1)
      // =====================================================
      val df =
        StockUtils
          .getDataFrame(spark)
          .sample(0.01)    // même logique que Phase 1
          .cache()

      val rowCount = df.count()

      // =====================================================
      // 2. FEATURE ENGINEERING DE BASE
      // =====================================================
      val enrichedDF = df
        .withColumn(
          "spread_lvl1",
          col("ask_price1") - col("bid_price1")
        )
        .withColumn(
          "spread_lvl2",
          col("ask_price2") - col("bid_price2")
        )
        .withColumn(
          "liquidity_lvl1",
          col("bid_size1") + col("ask_size1")
        )
        .withColumn(
          "order_imbalance",
          (col("bid_size1") - col("ask_size1")) /
          (col("bid_size1") + col("ask_size1"))
        )

      // =====================================================
      // 3. ANALYSES DESCRIPTIVES
      // =====================================================
      val spreadStats =
        enrichedDF
          .select("spread_lvl1", "spread_lvl2")
          .describe()

      val liquidityStats =
        enrichedDF
          .select("liquidity_lvl1")
          .describe()

      val imbalanceStats =
        enrichedDF
          .select("order_imbalance")
          .describe()

      val timeDistribution =
        enrichedDF
          .groupBy("seconds_in_bucket")
          .count()
          .orderBy("seconds_in_bucket")

      // =====================================================
      // 4. ÉCRITURE DES RÉSULTATS
      // =====================================================
      Using(
        new PrintWriter(
          new File(s"${StockUtils.OUTPUT_DIR}/stock_phase2_results.txt")
        )
      ) { writer =>

        writer.println(
          "PHASE 2 – ANALYSE DESCRIPTIVE & FEATURE ENGINEERING"
        )
        writer.println(
          "===================================================\n"
        )

        writer.println(
          s"Nombre de lignes analysées (échantillon) : $rowCount\n"
        )

        writer.println("Statistiques des spreads :")
        spreadStats.collect()
          .foreach(r => writer.println(r.mkString(" | ")))
        writer.println()

        writer.println("Statistiques de la liquidité (niveau 1) :")
        liquidityStats.collect()
          .foreach(r => writer.println(r.mkString(" | ")))
        writer.println()

        writer.println("Statistiques du déséquilibre acheteur/vendeur :")
        imbalanceStats.collect()
          .foreach(r => writer.println(r.mkString(" | ")))
        writer.println()

        writer.println("Distribution temporelle (seconds_in_bucket) :")
        timeDistribution.collect()
          .foreach { r =>
            writer.println(
              s" - seconde=${r.getInt(0)} : ${r.getLong(1)}"
            )
          }
      }

      println(
        s"Fichier ${StockUtils.OUTPUT_DIR}/stock_phase2_results.txt généré avec succès ✅"
      )

      df.unpersist()

    } catch {
      case e: Exception =>
        println(s"❌ Erreur Phase 2 : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
