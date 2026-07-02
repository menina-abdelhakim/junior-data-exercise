import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object GoldLayer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Gold Layer FHIR")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // LOAD SILVER
    val patients = spark.read.parquet("data/silver/patients")
    val adresses = spark.read.parquet("data/silver/adresses")
    val ipp = spark.read.parquet("data/silver/ipp")
    val opposition = spark.read.parquet("data/silver/opposition")

    // 1. RÉSOLUTION IPP
    val ippMapping = ipp
      .select(
        col("ipp").alias("source_ipp"),
        when(col("ipp_principal").isNotNull, col("ipp_principal"))
          .otherwise(col("ipp"))
          .alias("patient_id"),
        col("is_deprecated")
      )

    // 2. PATIENT MASTER
    val patientsMapped = patients
      .join(ippMapping, patients("ipp") === ippMapping("source_ipp"), "left")
      .withColumn("patient_id", coalesce(col("patient_id"), col("ipp")))
      .drop("source_ipp")

    val windowSpec = Window
      .partitionBy("patient_id")
      .orderBy(
        col("is_deprecated").asc,
        coalesce(col("date_fin_validite"), lit("9999-12-31")).desc
      )

    val patientsGold = patientsMapped
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank", "is_deprecated")
      .select(
        col("patient_id"),
        col("nom_naissance"),
        col("prenoms"),
        col("date_naissance"),
        col("sexe"),
        col("date_deces")
      )
      .withColumn(
        "prenoms",
        when(col("prenoms").isNull || size(col("prenoms")) === 0,
          array(lit("UNKNOWN"))
        ).otherwise(col("prenoms"))
      )
      .filter(col("nom_naissance").isNotNull)

    // 3. ADRESSES
    val adressesMapped = adresses
      .join(ippMapping, adresses("ipp") === ippMapping("source_ipp"), "left")
      .withColumn("patient_id", coalesce(col("patient_id"), col("ipp")))
      .drop("source_ipp")

    val adressesGold = adressesMapped
      .withColumn(
        "line_clean",
        upper(trim(regexp_replace(col("ligne_adresse"), "\\s+", " ")))
      )
      .withColumn(
        "rank",
        when(col("type_adresse") === "actuelle", 1)
          .when(col("type_adresse") === "domicile", 2)
          .otherwise(3)
      )
      .groupBy("patient_id")
      .agg(
        collect_set(
          struct(
            col("rank"),
            col("line_clean").alias("line"),
            col("code_postal").alias("postalCode"),
            col("ville").alias("city"),
            col("pays").alias("country"),
            col("type_adresse").alias("use_raw"),
            col("date_debut")
          )
        ).alias("addresses")
      )
      .withColumn(
        "address",
        expr("""
          array_distinct(
            transform(
              array_sort(addresses, (a, b) -> a.rank - b.rank),
              x ->
                struct(
                  x.line as line,
                  x.postalCode as postalCode,
                  x.city as city,
                  x.country as country,
                  case
                    when x.use_raw = 'actuelle' then 'home'
                    when x.use_raw = 'domicile' then 'home'
                    when x.use_raw = 'ancienne' then 'old'
                    else 'home'
                  end as use
                )
            )
          )
        """)
      )
      .select("patient_id", "address")

    // 4. OPPOSITION
    val oppositionMapped = opposition
      .join(ippMapping, opposition("ipp") === ippMapping("source_ipp"), "left")
      .withColumn("patient_id", coalesce(col("patient_id"), col("ipp")))
      .drop("source_ipp")

    val oppositionGold = oppositionMapped
      .groupBy("patient_id")
      .agg(
        max(when(col("opposition") === "O", true).otherwise(false))
          .alias("hasOpposition")
      )

    // 5. DATASET FINAL
    val goldDf = patientsGold
      .join(adressesGold, Seq("patient_id"), "left")
      .join(oppositionGold, Seq("patient_id"), "left")
      .withColumn(
        "active",
        when(col("date_deces").isNotNull, false).otherwise(true)
      )
      .withColumn(
        "hasOpposition",
        coalesce(col("hasOpposition"), lit(false))
      )

    // 6. CONSTRUCTION FHIR R4 – sans example.org et avec text minimal
    val fhirDf = goldDf.select(
      to_json(
        struct(
          lit("Patient").alias("resourceType"),
          col("patient_id").cast("string").alias("id"),

          // Ajout d'un text pour satisfaire dom-6 (avertissement)
          struct(
            lit("generated").alias("status"),
            lit("<div xmlns=\"http://www.w3.org/1999/xhtml\">Patient details</div>").alias("div")
          ).alias("text"),

          array(
            struct(
              lit("usual").alias("use"),
              lit("http://fhir.healthdata.fr/sid/ipp").alias("system"), // URL simple, pas example.org
              col("patient_id").cast("string").alias("value")
            )
          ).alias("identifier"),

          col("active"),

          array(
            struct(
              lit("official").alias("use"),
              col("nom_naissance").alias("family"),
              col("prenoms").alias("given")
            )
          ).alias("name"),

          when(col("sexe") === "H", "male")
            .when(col("sexe") === "F", "female")
            .otherwise("unknown")
            .alias("gender"),

          col("date_naissance").alias("birthDate"),
          col("date_deces").alias("deceasedDateTime"),

          col("address").alias("address"),

          when(
            col("hasOpposition") === true,
            array(
              struct(
                lit("http://fhir.healthdata.fr/StructureDefinition/opposition").alias("url"), // URL simple
                lit(true).alias("valueBoolean")
              )
            )
          ).otherwise(lit(null)).alias("extension")
        )
      ).alias("json")
    )

    // 7. OUTPUT en JSON Lines
    println("=== GOLD PATIENT FHIR (JSON Lines) ===")
    fhirDf.show(false)

    fhirDf.write
      .mode("overwrite")
      .text("data/gold/patients_fhir")

    spark.stop()
  }
}