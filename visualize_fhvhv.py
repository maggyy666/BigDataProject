"""
Proste wizualizacje dla danych FHVHV z agregatów Spark
"""
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

# Konfiguracja stylu
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def load_aggregates():
    """Wczytuje agregaty wygenerowane przez Spark"""
    agg_dir = Path("data/warehouse/aggregates/fhvhv")
    
    data = {}
    
    # Wczytaj metryki miesięczne
    monthly_csv_dir = agg_dir / "monthly_metrics_csv"
    if monthly_csv_dir.exists():
        csv_files = list(monthly_csv_dir.glob("*.csv"))
        if csv_files:
            data['monthly'] = pd.concat(
                [pd.read_csv(f) for f in csv_files],
                ignore_index=True
            )
            print(f"✅ Wczytano metryki miesięczne: {len(data['monthly'])} wierszy")
    
    # Wczytaj wolumen godzinowy
    hourly_csv_dir = agg_dir / "hourly_volume_csv"
    if hourly_csv_dir.exists():
        csv_files = list(hourly_csv_dir.glob("*.csv"))
        if csv_files:
            data['hourly'] = pd.concat(
                [pd.read_csv(f) for f in csv_files],
                ignore_index=True
            )
            print(f"✅ Wczytano wolumen godzinowy: {len(data['hourly'])} wierszy")
    
    return data

def create_visualizations(data, save_dir="data/analysis"):
    """Tworzy wizualizacje"""
    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Analiza danych FHVHV - Wyniki z Apache Spark', 
                 fontsize=16, fontweight='bold')
    
    # Wykres 1: Przychód miesięczny
    if 'monthly' in data:
        df_monthly = data['monthly']
        axes[0, 0].bar(df_monthly["pickup_month"], df_monthly["total_revenue"] / 1e6, 
                      color='#3498db')
        axes[0, 0].set_title('Przychód miesięczny', fontweight='bold')
        axes[0, 0].set_ylabel('Przychód (mln USD)')
        axes[0, 0].set_xlabel('Miesiąc')
        axes[0, 0].tick_params(axis='x', rotation=45)
        for i, v in enumerate(df_monthly["total_revenue"] / 1e6):
            axes[0, 0].text(i, v, f'${v:.1f}M', ha='center', va='bottom')
    
    # Wykres 2: Liczba przejazdów miesięcznie
    if 'monthly' in data:
        df_monthly = data['monthly']
        axes[0, 1].bar(df_monthly["pickup_month"], df_monthly["trips"] / 1e6, 
                      color='#e74c3c')
        axes[0, 1].set_title('Liczba przejazdów miesięcznie', fontweight='bold')
        axes[0, 1].set_ylabel('Liczba przejazdów (mln)')
        axes[0, 1].set_xlabel('Miesiąc')
        axes[0, 1].tick_params(axis='x', rotation=45)
        for i, v in enumerate(df_monthly["trips"] / 1e6):
            axes[0, 1].text(i, v, f'{v:.1f}M', ha='center', va='bottom')
    
    # Wykres 3: Średnia odległość i przychód per przejazd
    if 'monthly' in data:
        df_monthly = data['monthly']
        x = range(len(df_monthly))
        width = 0.35
        axes[1, 0].bar([i - width/2 for i in x], df_monthly["avg_trip_miles"], width, 
                      label='Śr. odległość', color='#2ecc71')
        ax2 = axes[1, 0].twinx()
        ax2.bar([i + width/2 for i in x], df_monthly["avg_revenue_per_trip"], width, 
               label='Śr. przychód', color='#f39c12')
        axes[1, 0].set_title('Średnia odległość i przychód per przejazd', fontweight='bold')
        axes[1, 0].set_ylabel('Odległość (mile)', color='#2ecc71')
        ax2.set_ylabel('Przychód (USD)', color='#f39c12')
        axes[1, 0].set_xlabel('Miesiąc')
        axes[1, 0].set_xticks(x)
        axes[1, 0].set_xticklabels(df_monthly["pickup_month"], rotation=45)
        axes[1, 0].legend(loc='upper left')
        ax2.legend(loc='upper right')
    
    # Wykres 4: Rozkład przejazdów w ciągu dnia (średnia ze wszystkich miesięcy)
    if 'hourly' in data:
        df_hourly = data['hourly']
        hourly_avg = df_hourly.groupby('pickup_hour')['trips'].mean()
        axes[1, 1].plot(hourly_avg.index, hourly_avg.values, marker='o', 
                       linewidth=2, markersize=6, color='#9b59b6')
        axes[1, 1].set_title('Średni rozkład przejazdów w ciągu dnia', fontweight='bold')
        axes[1, 1].set_xlabel('Godzina')
        axes[1, 1].set_ylabel('Średnia liczba przejazdów')
        axes[1, 1].set_xticks(range(0, 24, 2))
        axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_path = save_path / 'fhvhv_analysis.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\n✅ Zapisano wizualizację: {output_path}")
    plt.close()

def print_summary(data):
    """Wyświetla podsumowanie"""
    print("\n" + "=" * 80)
    print("📊 PODSUMOWANIE WYNIKÓW")
    print("=" * 80)
    
    if 'monthly' in data:
        df_monthly = data['monthly']
        print("\n💰 METRYKI MIESIĘCZNE:")
        print(df_monthly.to_string(index=False))
        
        total_revenue = df_monthly["total_revenue"].sum()
        total_trips = df_monthly["trips"].sum()
        print(f"\n   Całkowity przychód: ${total_revenue:,.2f}")
        print(f"   Całkowita liczba przejazdów: {total_trips:,}")

def main():
    print("=" * 80)
    print("📊 WIZUALIZACJA WYNIKÓW FHVHV Z APACHE SPARK")
    print("=" * 80)
    
    # Wczytaj dane
    data = load_aggregates()
    
    if not data:
        print("\n❌ Brak danych do wizualizacji!")
        print("   Uruchom najpierw: python spark_fhvhv_pipeline.py")
        return
    
    # Wyświetl podsumowanie
    print_summary(data)
    
    # Utwórz wizualizacje
    create_visualizations(data)
    
    print("\n✅ Gotowe!")

if __name__ == "__main__":
    main()

