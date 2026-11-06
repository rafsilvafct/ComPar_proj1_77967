"""
Uso:
    spark-submit run_project_pipeline.py --data ./data/ --output ./output/
"""

import argparse
import time
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession, Window


def build_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default="./data/",
                         help="Pasta contendo os ficheiros parquet dos táxis e o taxi_zone_lookup.csv")
    parser.add_argument("--output", default="./output/",
                         help="Pasta de destino para os resultados gravados")
    return parser


def run(spark, data_folder, output_folder):
    print("--- A carregar dados e a preparar o pipeline ---")

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")


    trips_path = f"{data_folder}/yellow_tripdata_*.parquet"
    zones_path = f"{data_folder}/taxi_zone_lookup.csv"

    zones_schema = StructType([
        StructField("LocationID", IntegerType(), False),
        StructField("Borough", StringType(), False),
        StructField("Zone", StringType(), False),
        StructField("service_zone", StringType(), False),
    ])

    zones_df = (spark.read.option("header", "true")
                .schema(zones_schema)
                .csv(zones_path))

    raw_df = spark.read.parquet(trips_path)
    
    # Preparar aliases para as zonas para facilitar os joins
    pickup_zones = zones_df.select(
        sf.col("LocationID").alias("pu_loc_id"), 
        sf.col("Borough").alias("pickup_borough"), 
        sf.col("Zone").alias("pickup_zone")
    )

    # -------------------------------------------------------------------------
    # report per-year cleaning statistics
    # -------------------------------------------------------------------------

    df_with_year = raw_df.withColumn("year", sf.year("tpep_pickup_datetime"))
    cleaning_stats = df_with_year.groupBy("year").agg(
        sf.count("*").alias("total_initial_records"),
        sf.sum(sf.when(sf.col("fare_amount") <= 0, 1).otherwise(0)).alias(
            "removed_fare_amount"
        ),
        sf.sum(sf.when(sf.col("trip_distance") <= 0, 1).otherwise(0)).alias(
            "removed_trip_distance"
        ),
        sf.sum(
            sf.when(
                (sf.col("passenger_count") == 0)
                | (sf.col("passenger_count") > 6),
                1,
            ).otherwise(0)
        ).alias("removed_passenger_count"),
        sf.sum(
            sf.when(
                sf.col("tpep_dropoff_datetime")
                <= sf.col("tpep_pickup_datetime"),
                1,
            ).otherwise(0)
        ).alias("removed_dates"),
        sf.sum(
            sf.when(
                ~sf.col("PULocationID").between(1, 263)
                | ~sf.col("DOLocationID").between(1, 263),
                1,
            ).otherwise(0)
        ).alias("removed_locations"),
    )

    cleaning_stats.show(truncate=False)

    # Base de dados limpa e enriquecida com colunas temporais e zonas já associadas
    base_df = (raw_df.filter(
            (sf.col("fare_amount") > 0) &
            (sf.col("trip_distance") > 0) &
            (sf.col("passenger_count") > 0) &
            (sf.col("passenger_count") <= 6) &
            (sf.col("tpep_dropoff_datetime") > sf.col("tpep_pickup_datetime")) &
            (sf.col("PULocationID").between(1, 263)) &
            (sf.col("DOLocationID").between(1, 263))
        )
        .withColumn("month", sf.month("tpep_pickup_datetime"))
        .withColumn("day_of_week", sf.date_format("tpep_pickup_datetime", "E"))
        .withColumn("hour_of_day", sf.hour("tpep_pickup_datetime"))
        .withColumn("trip_duration_mins", (sf.unix_timestamp("tpep_dropoff_datetime") - sf.unix_timestamp("tpep_pickup_datetime")) / 60.0)
        .join(pickup_zones.hint("broadcast"), sf.col("PULocationID") == sf.col("pu_loc_id"), "inner")
        .select("passenger_count", "trip_distance", "RatecodeID", "PULocationID", "DOLocationID", "payment_type",
                "fare_amount", "month", "day_of_week", "hour_of_day", "trip_duration_mins", "pickup_borough", "pickup_zone")
        .cache()
    )

    #Para obrigar a ir para a cache
    base_df.count()

    # -------------------------------------------------------------------------
    # M2
    # -------------------------------------------------------------------------
    print("A executar M2")
    (base_df
        .groupBy("pickup_borough", "day_of_week", "hour_of_day")
        .agg(
            sf.count("*").alias("total_trip_count"),
            sf.avg("fare_amount").alias("mean_fare_amount"),
            sf.avg("trip_duration_mins").alias("mean_trip_duration"),
            sf.avg("trip_distance").alias("mean_trip_distance")
        )
        .write.mode("overwrite")
        .partitionBy("pickup_borough")
        .parquet(f"{output_folder}/m2_temporal_demand")
    )

    # -------------------------------------------------------------------------
    # M3
    # -------------------------------------------------------------------------
    print("A executar M3")
    base_df_m3 = base_df.select("pickup_zone", "pickup_borough", "month", "fare_amount", "trip_distance")
    zone_month_stats = (base_df_m3
        .groupBy("pickup_zone", "pickup_borough", "month")
        .agg(
            sf.avg("fare_amount").alias("mean_fare"),
            sf.stddev("fare_amount").alias("stddev_fare"),
            sf.avg("trip_distance").alias("mean_distance"),
            sf.stddev("trip_distance").alias("stddev_distance"),
            sf.count("*").alias("monthly_trip_volume")
        )
    )

    w_borough_month = Window.partitionBy("pickup_borough", "month")
    w_zone_rolling = Window.partitionBy("pickup_borough", "pickup_zone").orderBy("month").rowsBetween(-2, 0)
    w_borough_rolling_time = Window.partitionBy("pickup_borough").orderBy("month").rowsBetween(-2, 0)

    f = (zone_month_stats
         .withColumn("borough_monthly_volume", sf.sum("monthly_trip_volume").over(w_borough_month))
         .withColumn("volume_rank", sf.rank().over(w_borough_month.orderBy(sf.desc("monthly_trip_volume"))))
         .withColumn("rolling_3m_zone_volume", sf.sum("monthly_trip_volume").over(w_zone_rolling))
         .withColumn("rolling_3m_borough_volume", sf.sum("borough_monthly_volume").over(w_borough_rolling_time))
         .withColumn("rolling_3m_trip_share", sf.col("rolling_3m_zone_volume") / sf.col("rolling_3m_borough_volume"))
         .withColumn("rolling_mean_volume", sf.avg("monthly_trip_volume").over(w_zone_rolling))
         .withColumn("rolling_stddev_volume", sf.stddev("monthly_trip_volume").over(w_zone_rolling))
         .withColumn("is_volume_deviating", sf.when(
            sf.col("rolling_stddev_volume").isNotNull() & (sf.col("rolling_stddev_volume") > 0),
            sf.abs(sf.col("monthly_trip_volume") - sf.col("rolling_mean_volume")) > (1.5 * sf.col("rolling_stddev_volume"))
        ).otherwise(False))
    )

    f.show()

    # -------------------------------------------------------------------------
    # O1
    # -------------------------------------------------------------------------
    print("A executar O1")
    o1_base = base_df.withColumn(
        "fare_per_dist", sf.col("fare_amount") / sf.col("trip_distance")
    ).withColumn(
        "is_weekend",
        sf.when(sf.col("day_of_week").isin("Sat", "Sun", "Sáb", "Dom"), sf.lit("weekend"))
        .otherwise(sf.lit("weekday"))
    ).select("PULocationID", "pickup_borough", "payment_type", "is_weekend", "fare_per_dist", "hour_of_day")

    window = Window.partitionBy("PULocationID", "hour_of_day")
    anomalies_df = (o1_base
        .withColumn("mean_fare_per_dist", sf.mean("fare_per_dist").over(window))
        .withColumn("std_fare_per_dist", sf.stddev("fare_per_dist").over(window))
        .withColumn("is_anomaly",
                    sf.when(
                        (sf.col("std_fare_per_dist").isNotNull()) &
                        (sf.col("std_fare_per_dist") > 0) &
                        (sf.col("fare_per_dist") > (
                                    sf.col("mean_fare_per_dist") + 3.0 * sf.col("std_fare_per_dist"))),
                        1
                    ).otherwise(0)
    ).select("pickup_borough", "payment_type", "is_weekend", "is_anomaly"))

    o1_result = anomalies_df.groupBy(
        "pickup_borough",
        "payment_type",
        "is_weekend"
    ).agg(
        sf.count("*").alias("total_trips"),
        sf.sum("is_anomaly").alias("anomaly_count"),
        (sf.sum("is_anomaly") / sf.count("*")).alias("anomaly_rate")
    )

    o1_result.show()

    # -------------------------------------------------------------------------
    # O4
    # -------------------------------------------------------------------------
    print("A executar O4")
    enriched_df = (base_df
                   .withColumn("is_airport", sf.when(sf.col("RatecodeID").isin([2, 3]), 1).otherwise(0))
                   .withColumn("is_negotiated", sf.when(sf.col("RatecodeID") == 5, 1).otherwise(0))
                   .withColumn("is_group", sf.when(sf.col("passenger_count") >= 4, 1).otherwise(0))
                   .select("trip_distance", "PULocationID", "fare_amount", "month", "hour_of_day", "is_airport", "is_negotiated", "is_group"))

    bucket_stats_df = enriched_df.groupBy("PULocationID", "hour_of_day").agg(
        sf.count("*").alias("total_bucket_trips"),
        sf.avg("is_airport").alias("frac_airport"),
        sf.avg("is_negotiated").alias("frac_negotiated"),
        sf.avg("is_group").alias("frac_group")
    )

    classified_buckets_df = bucket_stats_df.withColumn(
        "zone_type",
        sf.when(sf.col("frac_airport") > 0.3, "transit")
        .when(sf.col("frac_negotiated") > 0.2, "commercial")
        .when(sf.col("frac_group") > 0.15, "residential")
        .otherwise("mixed")
    )

    final_o4_df = enriched_df.join(
        classified_buckets_df.select("PULocationID", "hour_of_day", "zone_type").hint("broadcast"),
        on=["PULocationID", "hour_of_day"],
        how="inner"
    )

    o4_monthly_stats = final_o4_df.groupBy("zone_type", "month").agg(
        sf.avg("fare_amount").alias("mean_fare"),
        sf.avg("trip_distance").alias("mean_distance"),
        sf.count("*").alias("total_trips")
    )

    o4_monthly_stats.show()
    print("--- Pipeline Base Executado com Sucesso! ---")


def main():
    args = build_arg_parser().parse_args()

    spark = (SparkSession.builder
             .appName("compar-project1")
             .config("spark.driver.memory", "2g")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())

    start = time.perf_counter()
    run(spark, args.data, args.output)
    elapsed = time.perf_counter() - start
    print(f"Tempo total de execução (Wall time): {elapsed:.2f}s")

    spark.stop()


if __name__ == "__main__":
    main()