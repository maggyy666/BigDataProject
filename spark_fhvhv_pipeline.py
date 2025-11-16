"""
Minimalny pipeline Spark dla danych FHVHV (NYC, 2025-01..04)

- Wczytanie 4 plików Parquet
- Proste czyszczenie
- Metryki miesięczne i godzinowe
- Zapis do różnych katalogów / formatów (Parquet, CSV, JSON)
"""

from pathlib import Path
from pyspark.sql import SparkSession, functions as F

# --- ŚCIEŻKI ------------------------------------------------------------------

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
WH_DIR = DATA_DIR / "warehouse"
CLEANED_DIR = WH_DIR / "cleaned" / "fhvhv_2025_q1"
AGG_DIR = WH_DIR / "aggregates" / "fhvhv"
JSON_DIR = WH_DIR / "json_reports"

# --- SPARK --------------------------------------------------------------------

def create_spark():
    """Tworzy sesję Spark (local[*])"""
    spark = (
        SparkSession.builder
        .appName("NYC FHVHV Big Data Project")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# --- ETAP 1: WCZYTANIE --------------------------------------------------------

def load_fhvhv_data(spark):
    """Wczytuje 4 pliki FHVHV Parquet (2025-01..04)"""
    months = ["01", "02", "03", "04"]
    paths = [RAW_DIR / f"fhvhv_tripdata_2025-{m}.parquet" for m in months]

    print("\n📂 Wczytuję pliki FHVHV:")
    for p in paths:
        if p.exists():
            print(f"   ✓ {p.name}")
        else:
            print(f"   ⚠️  {p.name} - nie znaleziono")

    # Filtruj tylko istniejące pliki
    existing_paths = [p for p in paths if p.exists()]
    
    if not existing_paths:
        raise FileNotFoundError("Brak plików FHVHV do wczytania!")
    
    str_paths = [str(p) for p in existing_paths]
    df = spark.read.parquet(*str_paths)

    print(f"   → liczba wierszy RAW: {df.count():,}")
    return df

# --- ETAP 2: CZYSZCZENIE + DODATKOWE KOLUMNY ---------------------------------

def clean_data(df):
    """
    Proste czyszczenie:
    - wyrzucamy przejazdy bez dystansu / z ujemnym dystansem
    - obcinamy absurdalne wartości (np. > 200 mil)
    - wyrzucamy przejazdy z niepoprawną bazową opłatą
    - dodajemy kolumny czasowe + total_revenue
    """

    df_clean = (
        df
        # dystans sensowny
        .filter(F.col("trip_miles") > 0)
        .filter(F.col("trip_miles") < 200)
        # sensowna opłata pasażerska
        .filter(F.col("base_passenger_fare") > 0)
    )

    # total_revenue = suma wszystkich składowych + napiwek
    df_clean = df_clean.withColumn(
        "total_revenue",
        F.coalesce(F.col("base_passenger_fare"), F.lit(0))
        + F.coalesce(F.col("tolls"), F.lit(0))
        + F.coalesce(F.col("bcf"), F.lit(0))
        + F.coalesce(F.col("sales_tax"), F.lit(0))
        + F.coalesce(F.col("congestion_surcharge"), F.lit(0))
        + F.coalesce(F.col("airport_fee"), F.lit(0))
        + F.coalesce(F.col("cbd_congestion_fee"), F.lit(0))
        + F.coalesce(F.col("tips"), F.lit(0))
    )

    # kolumny czasowe: miesiąc, data, godzina
    df_clean = df_clean.withColumn(
        "pickup_datetime", F.to_timestamp("pickup_datetime")
    )
    df_clean = df_clean.withColumn(
        "pickup_month", F.date_format("pickup_datetime", "yyyy-MM")
    )
    df_clean = df_clean.withColumn(
        "pickup_date", F.to_date("pickup_datetime")
    )
    df_clean = df_clean.withColumn(
        "pickup_hour", F.hour("pickup_datetime")
    )

    print(f"   → liczba wierszy po czyszczeniu: {df_clean.count():,}")
    return df_clean

# --- ETAP 3: AGREGACJE --------------------------------------------------------

def aggregate_monthly(df):
    """Metryki miesięczne dla 4 miesięcy"""
    monthly = (
        df.groupBy("pickup_month")
        .agg(
            F.count("*").alias("trips"),
            F.sum("total_revenue").alias("total_revenue"),
            F.avg("total_revenue").alias("avg_revenue_per_trip"),
            F.sum("trip_miles").alias("total_miles"),
            F.avg("trip_miles").alias("avg_trip_miles"),
            F.sum("tips").alias("total_tips"),
            F.avg("tips").alias("avg_tip"),
        )
        .orderBy("pickup_month")
    )
    return monthly

def aggregate_hourly(df):
    """Liczba przejazdów per godzina w każdym miesiącu"""
    hourly = (
        df.groupBy("pickup_month", "pickup_hour")
        .agg(F.count("*").alias("trips"))
        .orderBy("pickup_month", "pickup_hour")
    )
    return hourly

def aggregate_summary(df):
    """Proste globalne podsumowanie dla całego okresu"""
    summary = df.agg(
        F.count("*").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("trip_miles").alias("total_miles"),
        F.avg("trip_miles").alias("avg_trip_miles"),
        F.avg("total_revenue").alias("avg_revenue_per_trip"),
        F.sum("tips").alias("total_tips"),
    )
    return summary

# --- ETAP 4: ZAPIS DO PARQUET / CSV / JSON -----------------------------------

def save_outputs(df_clean, monthly, hourly, summary):
    """Zapisuje wyniki w różnych lokalizacjach i formatach"""

    # katalogi
    for d in [CLEANED_DIR, AGG_DIR, JSON_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    # 1) oczyszczone dane – Parquet, partycjonowanie po miesiącu
    print(f"\n💾 Zapis oczyszczonych danych do Parquet: {CLEANED_DIR}")
    (
        df_clean.write
        .mode("overwrite")
        .partitionBy("pickup_month")
        .parquet(str(CLEANED_DIR))
    )

    # 2) metryki miesięczne – Parquet + CSV
    print(f"💾 Zapis miesięcznych metryk do: {AGG_DIR}")
    (
        monthly.write
        .mode("overwrite")
        .parquet(str(AGG_DIR / "monthly_metrics_parquet"))
    )
    (
        monthly.write
        .mode("overwrite")
        .option("header", True)
        .csv(str(AGG_DIR / "monthly_metrics_csv"))
    )

    # 3) wolumen godzinowy – CSV
    (
        hourly.write
        .mode("overwrite")
        .option("header", True)
        .csv(str(AGG_DIR / "hourly_volume_csv"))
    )

    # 4) globalne podsumowanie – JSON
    import json

    summary_pd = summary.toPandas()
    summary_path = JSON_DIR / "fhvhv_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_pd.to_dict("records"), f, indent=2, default=str)

    print(f"💾 Zapisano globalne podsumowanie: {summary_path}")

# --- MAIN ---------------------------------------------------------------------

def main():
    print("=" * 80)
    print("🚀 SPARK PIPELINE – NYC FHVHV 2025 Q1")
    print("=" * 80)

    spark = create_spark()

    try:
        # 1. wczytanie
        df_raw = load_fhvhv_data(spark)

        # 2. czyszczenie + kolumny pomocnicze
        df_clean = clean_data(df_raw)

        # 3. agregacje
        monthly = aggregate_monthly(df_clean)
        hourly = aggregate_hourly(df_clean)
        summary = aggregate_summary(df_clean)

        # 4. zapis
        save_outputs(df_clean, monthly, hourly, summary)

        print("\n" + "=" * 80)
        print("✅ Pipeline zakończony pomyślnie!")
        print("=" * 80)
        print(f"\n📁 Wyniki zapisane w:")
        print(f"   - {CLEANED_DIR}/ (oczyszczone dane Parquet)")
        print(f"   - {AGG_DIR}/ (agregaty Parquet + CSV)")
        print(f"   - {JSON_DIR}/ (raporty JSON)")

    except Exception as e:
        print(f"\n❌ Błąd: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

