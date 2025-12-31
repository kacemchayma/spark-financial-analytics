import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}

object StockPhase1 {

  def main(args: Array[String]): Unit = {

    val spark = StockUtils.getSparkSession("Stock Analysis - Phase 1 Ingestion")
    spark.sparkContext.setLogLevel("WARN")

    StockUtils.ensureOutputDir()

    try {
      // On lit le dataset (avec une limite pour éviter OOM si c'est trop gros)
      // Note: StockUtils.getDataFrame lit le dossier défini dans DATA_PATH
      val fullDF = StockUtils.getDataFrame(spark)
      
      // On travaille sur un échantillon pour la phase d'exploration si besoin
      // Mais ici on va essayer de compter le total d'abord
      println("Lecture des données en cours...")
      
      val totalRows = fullDF.count()
      
      // On prend un échantillon de 1 million pour les stats si c'est trop gros, 
      // mais pour l'instant on va utiliser le DF complet pour les agrégats simples
      val df = fullDF.limit(1000000)

      // Calculs de base sur le marché
      val stats = df.select(
        avg(col("ask_price1") - col("bid_price1")).alias("avg_spread1"),
        avg(col("ask_price2") - col("bid_price2")).alias("avg_spread2"),
        countDistinct("stock_id").alias("unique_stocks"),
        countDistinct("time_id").alias("unique_times")
      ).first()

      val nullCounts = df.select(
        df.columns.map(c =>
          sum(col(c).isNull.cast("int")).alias(c)
        ): _*
      ).first()

      // =====================================================
      // ÉCRITURE DU FICHIER TXT
      // =====================================================
      val writer = new PrintWriter(new File(s"${StockUtils.OUTPUT_DIR}/stock_phase1_results.txt"))

      writer.println("PHASE 1 – INGESTION ET EXPLORATION INITIALE (STOCK)")
      writer.println("==================================================\n")

      writer.println(s"Nombre total de lignes dans le dataset : $totalRows")
      writer.println(s"Analyse effectuée sur un échantillon de : ${df.count()} lignes\n")

      writer.println("Statistiques du marché :")
      writer.println(s" - Spread moyen (Niveau 1) : ${stats.get(0)}")
      writer.println(s" - Spread moyen (Niveau 2) : ${stats.get(1)}")
      writer.println(s" - Nombre d'actions uniques : ${stats.get(2)}")
      writer.println(s" - Nombre de créneaux temporels : ${stats.get(3)}\n")

      writer.println("Valeurs NULL par colonne (sur échantillon) :")
      df.columns.zipWithIndex.foreach { case (name, idx) =>
        writer.println(s" - $name : ${nullCounts.get(idx)}")
      }

      writer.close()

      println(s"Fichier ${StockUtils.OUTPUT_DIR}/stock_phase1_results.txt généré avec succès ✅")

    } catch {
      case e: Exception =>
        println(s"Erreur lors de la lecture des données : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
