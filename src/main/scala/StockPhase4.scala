import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans
import java.io.{File, PrintWriter}
import scala.util.Using

object StockPhase4 {

  def main(args: Array[String]): Unit = {

    val spark =
      StockUtils.getSparkSession(
        "Stock Analysis - Phase 4 MLlib Clustering"
      )
    spark.sparkContext.setLogLevel("WARN")

    StockUtils.ensureOutputDir()

    try {

      // =====================================================
      // 1. LECTURE
      // =====================================================
      val df = StockUtils.getDataFrame(spark)

      // =====================================================
      // 2. FEATURES TEMPORELLES (comme Phase 3)
      // =====================================================
      val cleanDF = df
        .filter(col("bid_price1") > 0)
        .filter(col("ask_price1") > 0)
        .filter(col("stock_id").isNotNull)
      
      val baseDF = cleanDF
        .withColumn("spread", col("ask_price1") - col("bid_price1"))
        .withColumn(
          "mid_price",
          (col("ask_price1") + col("bid_price1")) / 2
        )

      val windowSpec =
        Window
          .partitionBy("stock_id", "time_id")
          .orderBy("seconds_in_bucket")
          .rowsBetween(-5, 0)

      val lagWindow =
        Window
          .partitionBy("stock_id", "time_id")
          .orderBy("seconds_in_bucket")

      val enrichedDF = baseDF
        .withColumn("rolling_spread", avg("spread").over(windowSpec))
        .withColumn("rolling_volatility", stddev("mid_price").over(windowSpec))
        .withColumn(
          "price_change",
          col("mid_price") - lag("mid_price", 1).over(lagWindow)
        )

      // =====================================================
      // 3. AGRÉGATION PAR SESSION (DATASET ML)
      // =====================================================
      val sessionFeatures =
        enrichedDF
          .groupBy("time_id")
          .agg(
            avg("rolling_spread").alias("mean_spread"),
            avg("rolling_volatility").alias("mean_volatility"),
            avg(abs(col("price_change"))).alias("mean_price_change")
          )
          .na.drop()

      // =====================================================
      // 4. ASSEMBLAGE DES FEATURES
      // =====================================================
      val assembler =
        new VectorAssembler()
          .setInputCols(
            Array(
              "mean_spread",
              "mean_volatility",
              "mean_price_change"
            )
          )
          .setOutputCol("features")

      val featureDF = assembler.transform(sessionFeatures)

      // =====================================================
      // 5. KMEANS (CLUSTERING)
      // =====================================================
      val kmeans =
        new KMeans()
          .setK(3)                 // 3 régimes de marché
          .setSeed(42)
          .setFeaturesCol("features")
          .setPredictionCol("cluster")

      val model = kmeans.fit(featureDF)
      val clusteredDF = model.transform(featureDF)

      // =====================================================
      // 6. ANALYSE DES CLUSTERS
      // =====================================================
      val clusterStats =
        clusteredDF
          .groupBy("cluster")
          .agg(
            count("*").alias("nb_sessions"),
            avg("mean_spread").alias("avg_spread"),
            avg("mean_volatility").alias("avg_volatility"),
            avg("mean_price_change").alias("avg_price_change")
          )
          .orderBy("cluster")

      // =====================================================
      // 7. ÉCRITURE DES RÉSULTATS
      // =====================================================
      Using(
        new PrintWriter(
          new File(s"${StockUtils.OUTPUT_DIR}/stock_phase4_results.txt")
        )
      ) { writer =>

        writer.println(
          "PHASE 4 – SPARK MLLIB : CLUSTERING DES SESSIONS"
        )
        writer.println(
          "================================================\n"
        )

        writer.println("Méthode utilisée : KMeans")
        writer.println("Nombre de clusters : 3\n")

        writer.println("Statistiques par cluster :")

        clusterStats.collect().foreach { r =>
          writer.println(
            f"- Cluster ${r.getInt(0)} | " +
            s"sessions=${r.getLong(1)} | " +
            f"spread=${r.getDouble(2)}%.6f | " +
            f"volatilité=${r.getDouble(3)}%.6f | " +
            f"variation_prix=${r.getDouble(4)}%.6f"
          )
        }
      }

      println(
        s"Fichier ${StockUtils.OUTPUT_DIR}/stock_phase4_results.txt généré avec succès ✅"
      )

      df.unpersist()

    } catch {
      case e: Exception =>
        println(s"❌ Erreur Phase 4 : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
