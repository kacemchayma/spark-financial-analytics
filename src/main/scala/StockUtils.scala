import org.apache.spark.sql.SparkSession
import java.io.File

object StockUtils {

  // Configuration Hadoop pour Windows
  def setupHadoop(): Unit = {
    val projectDir = new File(".").getAbsolutePath
    System.setProperty("hadoop.home.dir", projectDir)
    
    // Hack pour contourner l'absence de winutils.exe et les erreurs de NativeIO sur Windows
    try {
      // Pour les versions de Hadoop 2.x/3.x
      val nativeIOClazz = Class.forName("org.apache.hadoop.io.nativeio.NativeIO")
      val nativeLoadedField = nativeIOClazz.getDeclaredField("nativeLoaded")
      nativeLoadedField.setAccessible(true)
      nativeLoadedField.set(null, false)

      val windowsClazz = Class.forName("org.apache.hadoop.io.nativeio.NativeIO$Windows")
      val skippedField = windowsClazz.getDeclaredField("skipped")
      skippedField.setAccessible(true)
      skippedField.set(null, true)
    } catch {
      case e: Exception => 
        println(s"Note: NativeIO hack failed: ${e.getMessage}")
    }
  }

  // Création de la Session Spark centralisée
  def getSparkSession(appName: String): SparkSession = {
    setupHadoop()
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "2g")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "512m")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.parquet.enableVectorizedReader", "false")
      .config("spark.sql.parquet.columnarReaderBatchSize", "100")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      .config("spark.hadoop.io.native.lib.available", "false")
      .getOrCreate()
  }

  // Chemins des données
  val DATA_PATH = "data/stock_id/stock_id.parquet"
  val OUTPUT_DIR = "output"

import org.apache.spark.sql.functions.lit

  // Helper pour lire les données
  def getDataFrame(spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
    // Le dataset ne contient pas stock_id, on l'ajoute artificiellement pour que le code fonctionne
    spark.read.parquet(DATA_PATH).withColumn("stock_id", lit(0))
  }

  // S'assurer que le dossier output existe
  def ensureOutputDir(): Unit = {
    val dir = new File(OUTPUT_DIR)
    if (!dir.exists()) dir.mkdirs()
  }
}
