import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BronzeLayer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Bronze Layer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // =====================================================
    // UTIL : suppression des colonnes parasites (_c0, _c1...)
    // =====================================================
    def removeExtraColumns(df: org.apache.spark.sql.DataFrame) = {
      val colsToDrop = df.columns.filter(_.startsWith("_c"))
      df.drop(colsToDrop: _*)
    }

    // =====================================================
    // PATIENTS
    // =====================================================
    val patientsBronze = removeExtraColumns(
      spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .option("mode", "PERMISSIVE")
        .csv("resources/patients.csv")
    )

    // =====================================================
    // ADRESSES
    // =====================================================
    val adressesBronze = removeExtraColumns(
      spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv("resources/adresses.csv")
    )

    // =====================================================
    // IPP
    // =====================================================
    val ippBronze = removeExtraColumns(
      spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv("resources/identifiants_ipp.csv")
    )

    // =====================================================
    // OPPOSITION
    // =====================================================
    val oppositionBronze = removeExtraColumns(
      spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv("resources/opposition_recherche.csv")
    )

    // =====================================================
    // OUTPUT
    // =====================================================
    println("=== PATIENTS BRONZE ===")
    patientsBronze.show(false)

    println("=== ADRESSES BRONZE ===")
    adressesBronze.show(false)

    println("=== IPP BRONZE ===")
    ippBronze.show(false)

    println("=== OPPOSITION BRONZE ===")
    oppositionBronze.show(false)

    patientsBronze.write
      .mode("overwrite")
      .parquet("data/bronze/patients")

    adressesBronze.write
      .mode("overwrite")
      .parquet("data/bronze/adresses")

    ippBronze.write
      .mode("overwrite")
      .parquet("data/bronze/ipp")

    oppositionBronze.write
      .mode("overwrite")
      .parquet("data/bronze/opposition")

    spark.stop()
  }
}