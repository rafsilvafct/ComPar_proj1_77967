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
    
    dropoff_zones = zones_df.select(
        sf.col("LocationID").alias("do_loc_id"), 
        sf.col("Borough").alias("dropoff_borough"), 
        sf.col("Zone").alias("dropoff_zone")
    )

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
        .join(pickup_zones, sf.col("PULocationID") == sf.col("pu_loc_id"), "inner")
        .join(dropoff_zones, sf.col("DOLocationID") == sf.col("do_loc_id"), "inner")
    )

    # -------------------------------------------------------------------------
    # M2 - Temporal demand analysis[cite: 3, 4]
    # -------------------------------------------------------------------------
    print("A executar M2: Análise de Procura Temporal...")
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
    # M3 - Zone-level statistics[cite: 4]
    # -------------------------------------------------------------------------
    print("A executar M3: Estatísticas ao Nível da Zona...")
    zone_month_stats = (base_df
        .groupBy("pickup_zone", "pickup_borough", "month")
        .agg(
            sf.avg("fare_amount").alias("mean_fare"),
            sf.stddev("fare_amount").alias("stddev_fare"),
            sf.avg("trip_distance").alias("mean_distance"),
            sf.stddev("trip_distance").alias("stddev_distance"),
            sf.count("*").alias("monthly_trip_volume")
        )
    )

    w_borough = Window.partitionBy("pickup_borough", "month")
    w_rolling = Window.partitionBy("pickup_zone").orderBy("month").rowsBetween(-2, 0)

    (zone_month_stats
        .withColumn("volume_rank", sf.rank().over(w_borough.orderBy(sf.desc("monthly_trip_volume"))))
        .withColumn("rolling_3m_volume", sf.avg("monthly_trip_volume").over(w_rolling))
        .write.mode("overwrite")
        .parquet(f"{output_folder}/m3_zone_stats")
    )

    # -------------------------------------------------------------------------
    # O1 - Fare anomaly detection[cite: 4]
    # -------------------------------------------------------------------------
    print("A executar O1: Deteção de Anomalias nas Tarifas...")
    df_o1 = base_df.withColumn("fare_per_km", sf.col("fare_amount") / sf.col("trip_distance"))
    
    o1_bucket_stats = (df_o1
        .groupBy("PULocationID", "hour_of_day")
        .agg(
            sf.avg("fare_per_km").alias("mean_fpk"),
            sf.stddev("fare_per_km").alias("std_fpk")
        )
    )
    
    (df_o1.join(o1_bucket_stats, ["PULocationID", "hour_of_day"])
        .withColumn("is_anomaly", sf.col("fare_per_km") > (sf.col("mean_fpk") + 3 * sf.col("std_fpk")))
        .write.mode("overwrite")
        .parquet(f"{output_folder}/o1_fare_anomalies")
    )

    # -------------------------------------------------------------------------
    # O2 - Origin-destination flow analysis[cite: 4]
    # -------------------------------------------------------------------------
    print("A executar O2: Análise de Fluxos Origem-Destino...")
    od_flows = (base_df
        .groupBy("month", "PULocationID", "DOLocationID", "pickup_borough", "dropoff_borough")
        .agg(sf.count("*").alias("route_trip_count"))
    )
    
    w_outbound = Window.partitionBy("PULocationID", "month")
    (od_flows
        .withColumn("total_outbound", sf.sum("route_trip_count").over(w_outbound))
        .withColumn("route_share", sf.col("route_trip_count") / sf.col("total_outbound"))
        .write.mode("overwrite")
        .parquet(f"{output_folder}/o2_od_flows")
    )

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