# System do przechowywania i analizy Big Data - NYC Taxi

Projekt studencki implementujący dwustopniowy system analizy danych taksówek z Nowego Jorku:
- **Warstwa Big Data (Apache Spark)**: ETL, czyszczenie, agregacje, zapis do różnych formatów
- **Warstwa analityczna (Pandas)**: Wizualizacje i raporty na podstawie przetworzonych danych

## 📋 Wymagania

- Python 3.8+
- Java JDK 8+ (wymagane przez Spark)
- Apache Spark 3.5.0+ (instalowany przez PySpark)

## 🚀 Instalacja

1. **Sklonuj repozytorium lub pobierz pliki projektu**

2. **Utwórz środowisko wirtualne:**
   ```bash
   python -m venv venv
   ```

3. **Aktywuj środowisko wirtualne:**
   - Windows: `venv\Scripts\activate`
   - Linux/Mac: `source venv/bin/activate`

4. **Zainstaluj zależności:**
   ```bash
   pip install -r requirements.txt
   ```

5. **Pobierz dane:**
   - Umieść pliki Parquet z NYC TLC w katalogu `data/raw/`
   - Przykładowe pliki:
     - `yellow_tripdata_2025-01.parquet`
     - `green_tripdata_2025-01.parquet`
     - `fhv_tripdata_2025-01.parquet`
     - `fhvhv_tripdata_2025-01.parquet`

## 📁 Struktura projektu

```
BigDataProject/
│
├─ data/
│   ├─ raw/              # Oryginalne pliki Parquet z NYC TLC
│   ├─ warehouse/        # Dane przetworzone przez Spark
│   │   ├─ cleaned/      # Oczyszczone dane (Parquet, partycjonowane)
│   │   ├─ aggregates/   # Agregaty (Parquet + CSV)
│   │   └─ json_reports/ # Raporty JSON ze statystykami
│   └─ analysis/         # Wyniki analizy (PNG + TXT)
│
├─ spark_pipeline.py     # Główny pipeline Spark (ETL + agregacje)
├─ analyze_taxi_data.py  # Analiza biznesowa + wizualizacje
├─ view_parquet.py       # Narzędzie do przeglądania plików Parquet
└─ requirements.txt      # Zależności Python
```

## 🔧 Użycie

### Krok 1: Przetwarzanie danych w Spark

Uruchom pipeline Spark, który:
- Wczyta dane z `data/raw/`
- Oczyści dane (usunie błędne wartości)
- Wykona agregacje (dzienny przychód, przejazdy per godzina, statystyki)
- Zapisze wyniki do różnych lokalizacji i formatów

```bash
python spark_pipeline.py
```

**Wyniki:**
- `data/warehouse/cleaned/` - oczyszczone dane Parquet (partycjonowane po dacie)
- `data/warehouse/aggregates/daily_revenue_parquet/` - dzienny przychód (Parquet)
- `data/warehouse/aggregates/daily_revenue_csv/` - dzienny przychód (CSV)
- `data/warehouse/aggregates/hourly_trips_csv/` - przejazdy per godzina (CSV)
- `data/warehouse/json_reports/distance_stats.json` - statystyki odległości (JSON)
- `data/warehouse/json_reports/financial_stats.json` - statystyki finansowe (JSON)

### Krok 2: Analiza i wizualizacje

#### Opcja A: Analiza na agregatach z Sparka (zalecane)

Używa przetworzonych danych z Sparka - szybsze i bardziej efektywne:

```bash
python analyze_taxi_data.py --spark
```

#### Opcja B: Analiza na surowych danych

Analizuje oryginalne pliki Parquet bezpośrednio (wymaga więcej pamięci):

```bash
python analyze_taxi_data.py
```

**Wyniki:**
- `data/analysis/spark_aggregates_analysis.png` - wizualizacje z agregatów Sparka
- `data/analysis/financial_analysis.png` - analiza finansowa
- `data/analysis/distance_analysis.png` - analiza odległości
- `data/analysis/time_patterns_analysis.png` - wzorce czasowe
- `data/analysis/business_analysis_report.txt` - raport tekstowy

### Przeglądanie plików Parquet

```bash
python view_parquet.py
# lub z konkretną ścieżką:
python view_parquet.py "data/raw/yellow_tripdata_2025-01.parquet"
```

### Sprawdzanie środowiska

Przed uruchomieniem Spark pipeline, sprawdź czy wszystko jest OK:

```bash
python check_environment.py
```

Skrypt sprawdzi:
- Wersję Java (wymagana 8, 11 lub 17)
- Zmienną JAVA_HOME
- Zainstalowany PySpark
- Dostępność plików danych

### Prosta wizualizacja wyników Spark

Po uruchomieniu `spark_pipeline.py`, możesz szybko wygenerować wykresy:

```bash
python visualize_spark_results.py
```

Tworzy wizualizacje z agregatów wygenerowanych przez Spark.

## 📊 Architektura systemu

### Warstwa Big Data (Apache Spark)

**Funkcje:**
- Import dużych zbiorów danych Parquet (miliony wierszy)
- Czyszczenie danych (filtrowanie błędnych wartości)
- Agregacje (dzienne, godzinowe, statystyczne)
- Zapis do różnych formatów:
  - **Parquet** - dla dalszego przetwarzania (z partycjonowaniem)
  - **CSV** - dla łatwego importu do Pandas
  - **JSON** - dla raportów i dokumentacji

**Konfiguracja Spark:**
- Tryb: `local[*]` (wykorzystuje wszystkie dostępne rdzenie)
- Adaptive Query Execution (AQE) włączony
- Logowanie na poziomie WARN

### Warstwa analityczna (Pandas + Matplotlib)

**Funkcje:**
- Wczytywanie agregatów z Sparka (CSV, JSON)
- Analiza biznesowa (przychody, odległości, wzorce czasowe)
- Wizualizacje (wykresy liniowe, słupkowe, boxploty)
- Generowanie raportów tekstowych

## 📈 Przykładowe analizy

### Agregacje w Spark:

1. **Dzienny przychód:**
   - Liczba przejazdów per dzień
   - Całkowity przychód dzienny
   - Średnia kwota za przejazd

2. **Przejazdy per godzina:**
   - Rozkład przejazdów w ciągu dnia
   - Identyfikacja godzin szczytu

3. **Statystyki odległości:**
   - Średnia, mediana, min, max
   - Całkowita przejechana odległość

4. **Statystyki finansowe:**
   - Średnie opłaty i napiwki
   - Całkowity przychód
   - Liczba przejazdów z napiwkiem

## 🔍 Informacje techniczne

### Wersje bibliotek:
- PySpark: 3.5.0+
- Pandas: 2.0.0+
- Matplotlib: 3.7.0+
- Seaborn: 0.12.0+
- PyArrow: 10.0.0+ (wymagane do odczytu Parquet)

### Wymagania systemowe:
- **Pamięć RAM:** Minimum 8GB (zalecane 16GB+ dla pełnego miesiąca danych)
- **Dysk:** ~2-5GB wolnego miejsca na przetworzone dane
- **CPU:** Wielordzeniowy procesor (Spark wykorzystuje wszystkie rdzenie)

## 📝 Sprawozdanie

Projekt spełnia wymagania:
- ✅ System do przechowywania i analizy Big Data w oparciu o Apache Spark
- ✅ Import i składowanie w różnych lokalizacjach i formatach
- ✅ Analiza dużych zbiorów danych (miliony wierszy)
- ✅ Agregacje i obliczenia w Spark
- ✅ Wizualizacje i raporty w Pandas

## 🐛 Rozwiązywanie problemów

### Błąd: "UnsupportedOperationException: getSubject"
**Problem:** Masz zbyt nową wersję Java (21+). Spark wymaga JDK 8, 11 lub 17 (LTS).

**Rozwiązanie:**
1. Sprawdź wersję Java: `java -version`
2. Jeśli masz Java 21+, zainstaluj **JDK 17 (LTS)** z [Adoptium](https://adoptium.net/)
3. Ustaw `JAVA_HOME` na katalog z JDK 17
4. Dodaj `%JAVA_HOME%\bin` do PATH (na początku listy)
5. **Zamknij i otwórz nowy terminal** (zmienne środowiskowe się odświeżą)
6. Sprawdź ponownie: `java -version` (powinno pokazać 17.x)

**Uruchom diagnostykę:**
```bash
python check_environment.py
```

### Błąd: "Java not found"
- Zainstaluj Java JDK 17 (LTS) z [Adoptium](https://adoptium.net/)
- Ustaw zmienną środowiskową `JAVA_HOME`

### Błąd: "Out of memory"
- Zmniejsz rozmiar danych lub zwiększ pamięć dla Spark:
  ```python
  .config("spark.driver.memory", "4g")
  ```

### Błąd: "File not found"
- Sprawdź czy pliki Parquet są w `data/raw/`
- Sprawdź czy uruchomiłeś `spark_pipeline.py` przed analizą agregatów

### Warningi: "winutils.exe" i "native-hadoop library"
- **Można zignorować** na Windowsie - Spark używa wbudowanych klas
- Te warningi nie wpływają na działanie pipeline

## 📚 Źródła danych

Dane pochodzą z [NYC Taxi & Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## 📄 Licencja

Zobacz plik LICENSE
