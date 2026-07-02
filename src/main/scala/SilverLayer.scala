import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

object SilverLayer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Silver Layer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // =========================
    // UTIL : DATE NORMALIZATION
    // =========================
    def normalizeDate(c: Column): Column =
      date_format(
        coalesce(
          to_date(c, "yyyy-MM-dd"),
          to_date(c, "dd/MM/yyyy"),
          to_date(c, "dd-MM-yyyy"),
          to_date(c, "yyyy/MM/dd")
        ),
        "yyyy-MM-dd"
      )

    // =========================
    // UTIL : CLEAN STRING
    // =========================
    def clean(c: Column): Column =
      trim(regexp_replace(c, "\\s+", " "))

    // =====================================================
    // LOAD BRONZE
    // =====================================================

    val patientsRaw = spark.read.parquet("data/bronze/patients")
    val adressesRaw = spark.read.parquet("data/bronze/adresses")
    val ippRaw = spark.read.parquet("data/bronze/ipp")
    val oppositionRaw = spark.read.parquet("data/bronze/opposition")

    // =====================================================
    // PATIENTS
    // =====================================================

    val patientsSilver = patientsRaw
      .withColumn("ipp", col("ipp").cast(LongType))
      .withColumn("nom_naissance", upper(clean(col("nom_naissance"))))
      .withColumn("nom_usuel", upper(clean(col("nom_usuel"))))
      .withColumn(
        "sexe",
        when(
          lower(trim(col("sexe")))
            .isin("m", "h", "homme", "male", "1"),
          "H"
        )
          .when(
            lower(trim(col("sexe")))
              .isin("f", "femme", "female", "2"),
            "F"
          )
          .otherwise(null)
      )
      .withColumn("date_naissance", normalizeDate(col("date_naissance")))
      .withColumn("date_deces", normalizeDate(col("date_deces")))
      .withColumn("date_fin_validite", normalizeDate(col("date_fin_validite")))
      .withColumn(
        "prenoms",
        expr("""
          array_distinct(
            transform(
              split(regexp_replace(prenoms, '[\\[\\]\"]', ''), ','),
              p ->
                concat_ws(
                  '-',
                  transform(
                    split(trim(p), '-'),
                    x -> concat(
                      upper(substr(lower(x), 1, 1)),
                      substr(lower(x), 2)
                    )
                  )
                )
            )
          )
        """)
      )

    // =====================================================
    // ADRESSES
    // =====================================================

    val adressesSilver = adressesRaw
      .withColumn("ipp", col("ipp").cast(LongType))
      .withColumn("ville", upper(clean(col("ville"))))
      .withColumn("pays", upper(trim(col("pays"))))
      .withColumn("date_debut", normalizeDate(col("date_debut")))
      .withColumn("date_fin", normalizeDate(col("date_fin")))

    // =====================================================
    // IPP
    // =====================================================

    val ippSilver = ippRaw
      .withColumn("ipp", col("ipp").cast(LongType))
      .withColumn("ipp_principal", col("ipp_principal").cast(LongType))
      .withColumn(
        "statut",
        when(
          lower(trim(col("statut")))
            .isin("deprecie", "déprécié"),
          "DEPRECIE"
        )
          .when(
            lower(trim(col("statut"))).contains("actif"),
            "ACTIF"
          )
          .otherwise(null)
      )
      .withColumn(
        "is_deprecated",
        when(col("statut") === "DEPRECIE", true)
          .otherwise(false)
      )
      .withColumn(
        "date_modification",
        normalizeDate(col("date_modification"))
      )

    // =====================================================
    // OPPOSITION
    // =====================================================

    val oppositionSilver = oppositionRaw
      .withColumn("ipp", col("ipp").cast(LongType))
      .withColumn("date_recueil", normalizeDate(col("date_recueil")))
      .withColumn(
        "opposition",
        when(
          lower(trim(col("opposition")))
            .isin("o", "oui", "true", "1", "opposé"),
          "O"
        )
          .when(
            lower(trim(col("opposition")))
              .isin("n", "non", "false", "0"),
            "N"
          )
          .otherwise(null)
      )

    // =====================================================
    // OUTPUT
    // =====================================================

    println("=== PATIENTS SILVER ===")
    patientsSilver.show(false)

    println("=== ADRESSES SILVER ===")
    adressesSilver.show(false)

    println("=== IPP SILVER ===")
    ippSilver.show(false)

    println("=== OPPOSITION SILVER ===")
    oppositionSilver.show(false)

    patientsSilver.write
      .mode("overwrite")
      .parquet("data/silver/patients")

    adressesSilver.write
      .mode("overwrite")
      .parquet("data/silver/adresses")

    ippSilver.write
      .mode("overwrite")
      .parquet("data/silver/ipp")

    oppositionSilver.write
      .mode("overwrite")
      .parquet("data/silver/opposition")

    spark.stop()
  }
}