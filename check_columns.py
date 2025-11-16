"""
Prosty skrypt do sprawdzania nazw kolumn w plikach Parquet
"""
import pandas as pd
from pathlib import Path

def check_columns():
    """Sprawdza nazwy kolumn we wszystkich plikach Parquet w data/raw"""
    raw_dir = Path("data/raw")
    
    if not raw_dir.exists():
        print(f"❌ Katalog {raw_dir} nie istnieje")
        return
    
    parquet_files = list(raw_dir.glob("*.parquet"))
    
    if not parquet_files:
        print(f"⚠️  Brak plików Parquet w {raw_dir}")
        return
    
    print("=" * 80)
    print("📋 NAZWY KOLUMN W PLIKACH PARQUET")
    print("=" * 80)
    
    for file_path in sorted(parquet_files):
        print(f"\n📄 {file_path.name}")
        print("-" * 80)
        
        try:
            # Wczytaj tylko schemat (bez danych)
            df = pd.read_parquet(file_path, engine='pyarrow')
            
            print(f"Liczba kolumn: {len(df.columns)}")
            print(f"Liczba wierszy: {len(df):,}")
            print(f"\nKolumny:")
            for i, col in enumerate(df.columns, 1):
                dtype = df[col].dtype
                print(f"  {i:2d}. {col:30s} ({dtype})")
        
        except Exception as e:
            print(f"❌ Błąd podczas wczytywania: {e}")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    check_columns()

