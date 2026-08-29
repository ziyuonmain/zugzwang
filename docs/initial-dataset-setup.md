# Initial dataset setup

This guide bootstraps the **June 2026** source data required by Zugzwang's first end-to-end Databricks vertical slice.

It prepares three source families locally under `data/landing/`:

```text
data/landing/
├── railway/
│   └── data-2026-06.parquet
├── stada/
│   └── stada_stations.json
└── dwd/
    ├── metadata/
    │   ├── TU_Stundenwerte_Beschreibung_Stationen.txt
    │   └── FF_Stundenwerte_Beschreibung_Stationen.txt
    ├── temperature/
    │   └── produkt_tu_stunde_*.txt
    └── wind/
        └── produkt_ff_stunde_*.txt
```

The raw files are then uploaded to the development Unity Catalog Volume:

```text
/Volumes/zugzwang_dev/raw/landing/
```

> Run all commands from the repository root unless stated otherwise.

## Prerequisites

Required local tools:

```bash
curl --version
uv --version
wget --version
unzip -v
Databricks -v
```

If `wget` or `unzip` is missing on Debian/Ubuntu/Linux Mint:

```bash
sudo apt update
sudo apt install wget unzip
```

Make sure local source data is ignored by Git:

```gitignore
/data/
```

Create the local landing structure:

```bash
mkdir -p \
  data/landing/railway \
  data/landing/stada \
  data/landing/dwd/metadata \
  data/landing/dwd/temperature \
  data/landing/dwd/wind
```

## Railway operational data

Source: `piebro/deutsche-bahn-data` on Hugging Face.

The project publishes monthly processed Parquet releases under `monthly_processed_data/`.

Download the June 2026 release:

```bash
curl -L \
  'https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-2026-06.parquet' \
  -o data/landing/railway/data-2026-06.parquet
```

Verify the file exists:

```bash
ls -lh data/landing/railway/data-2026-06.parquet
```

Optional: verify the expected row count with DuckDB without installing it permanently:

```bash
uv run --with duckdb python <<'PY'
import duckdb

path = 'data/landing/railway/data-2026-06.parquet'
count = duckdb.sql(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
print(f'Rows: {count:,}')
PY
```

Expected June 2026 row count:

```text
14,752,336
```

## DB StaDa station snapshot

For the June 2026 vertical slice, use the **contemporaneous StaDa snapshot archived by `piebro/deutsche-bahn-data`** instead of mixing June operational facts with a later station-master snapshot.

Download the raw piebro archive file used by the feasibility spike:

```bash
curl -L \
  'https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/raw_data/year=2026/month=6/day=1/hour_00_19_20_21_22_23.parquet' \
  -o /tmp/zugzwang-piebro-2026-06-01.parquet
```

Extract the successful `station-data/v2/stations` responses and consolidate the station records into one JSON array:

```bash
uv run --with pandas --with pyarrow python <<'PY'
import json
from pathlib import Path

import pandas as pd

source = Path('/tmp/zugzwang-piebro-2026-06-01.parquet')
target = Path('data/landing/stada/stada_stations.json')

df = pd.read_parquet(source)

stada = df[df['api_name'] == 'station-data/v2/stations']

if 'status_code' in stada.columns:
    stada = stada[stada['status_code'].astype(str) == '200']

stations = {}

for response in stada['response_data'].dropna():
    payload = json.loads(response)

    for station in payload.get('result', []):
        station_number = station.get('number')
        if station_number is not None:
            stations[str(station_number)] = station

target.parent.mkdir(parents=True, exist_ok=True)
with target.open('w', encoding='utf-8') as file:
    json.dump(list(stations.values()), file, ensure_ascii=False, indent=2)

print(f'Wrote {len(stations):,} stations to {target}')
PY
```

The feasibility spike observed approximately:

```text
5,462 station records
```

Verify the generated file:

```bash
ls -lh data/landing/stada/stada_stations.json
```

Optional count check:

```bash
uv run python <<'PY'
import json

with open('data/landing/stada/stada_stations.json', encoding='utf-8') as file:
    stations = json.load(file)

print(f'Stations: {len(stations):,}')
PY
```

## DWD station metadata

Source: Deutscher Wetterdienst (DWD) Climate Data Center (CDC).

The June 2026 observations are still available from the DWD `recent/` hourly archives. The `historical/` archives generally end at 2025-12-31, so use `recent/` for this bootstrap.

Download the hourly air-temperature station metadata:

```bash
curl -L \
  'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/TU_Stundenwerte_Beschreibung_Stationen.txt' \
  -o data/landing/dwd/metadata/TU_Stundenwerte_Beschreibung_Stationen.txt
```

Download the hourly wind station metadata:

```bash
curl -L \
  'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/recent/FF_Stundenwerte_Beschreibung_Stationen.txt' \
  -o data/landing/dwd/metadata/FF_Stundenwerte_Beschreibung_Stationen.txt
```

Verify both files:

```bash
ls -lh data/landing/dwd/metadata/
```

## DWD hourly temperature observations

Download the currently published hourly temperature archives into a temporary directory:

```bash
mkdir -p /tmp/zugzwang-dwd/temperature

wget \
  --recursive \
  --no-parent \
  --no-directories \
  --accept='stundenwerte_TU_*_akt.zip' \
  --directory-prefix=/tmp/zugzwang-dwd/temperature \
  'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/'
```

Count the downloaded ZIP archives:

```bash
find /tmp/zugzwang-dwd/temperature -type f -name '*.zip' | wc -l
```

Extract only the observation files required by the pipeline:

```bash
for zip in /tmp/zugzwang-dwd/temperature/*.zip; do
  unzip -jo "$zip" 'produkt_tu_stunde_*.txt' \
    -d data/landing/dwd/temperature/
done
```

Count and inspect the extracted files:

```bash
find data/landing/dwd/temperature -type f -name 'produkt_tu_stunde_*.txt' | wc -l
```

```bash
head "$(find data/landing/dwd/temperature -type f -name 'produkt_tu_stunde_*.txt' | head -1)"
```

Typical columns include:

```text
STATIONS_ID
MESS_DATUM
QN_9
TT_TU
RF_TU
```

Do **not** rewrite the downloaded files to contain only June records. Keep the raw DWD source files unchanged; the Lakeflow Silver transformation is responsible for selecting June 2026 and converting `-999` / `-999.0` missing-value sentinels to nulls.

## DWD hourly wind observations

Download the currently published hourly wind archives:

```bash
mkdir -p /tmp/zugzwang-dwd/wind

wget \
  --recursive \
  --no-parent \
  --no-directories \
  --accept='stundenwerte_FF_*_akt.zip' \
  --directory-prefix=/tmp/zugzwang-dwd/wind \
  'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/recent/'
```

Count the downloaded ZIP archives:

```bash
find /tmp/zugzwang-dwd/wind -type f -name '*.zip' | wc -l
```

Extract only the hourly wind observation files:

```bash
for zip in /tmp/zugzwang-dwd/wind/*.zip; do
  unzip -jo "$zip" 'produkt_ff_stunde_*.txt' \
    -d data/landing/dwd/wind/
done
```

Count and inspect them:

```bash
find data/landing/dwd/wind -type f -name 'produkt_ff_stunde_*.txt' | wc -l
```

```bash
head "$(find data/landing/dwd/wind -type f -name 'produkt_ff_stunde_*.txt' | head -1)"
```

Typical columns include:

```text
STATIONS_ID
MESS_DATUM
QN_3
F
D
```

As with temperature, preserve the raw observations and let the Silver transformation filter the June 2026 time window.

## Verify the local landing dataset

Inspect the tree:

```bash
tree data/landing
```

List representative files:

```bash
find data/landing -type f | sort | head -30
```

Count all files:

```bash
find data/landing -type f | wc -l
```

Check total local size:

```bash
du -sh data/landing
```

The resulting layout should resemble:

```text
data/landing/
├── railway/
│   └── data-2026-06.parquet
├── stada/
│   └── stada_stations.json
└── dwd/
    ├── metadata/
    │   ├── TU_Stundenwerte_Beschreibung_Stationen.txt
    │   └── FF_Stundenwerte_Beschreibung_Stationen.txt
    ├── temperature/
    │   ├── produkt_tu_stunde_....txt
    │   └── ...
    └── wind/
        ├── produkt_ff_stunde_....txt
        └── ...
```

## Upload to the Databricks landing volume

The Databricks bundle is responsible for creating the Unity Catalog catalog/schema/Volume objects. The dataset bootstrap only uploads files into the already-created `landing` Volume.

Verify the Databricks CLI profile/workspace first:

```bash
databricks auth profiles
```

If needed, specify your profile on all commands with `--profile <profile-name>`.

Upload the complete local landing tree recursively:

```bash
databricks fs cp \
  data/landing/ \
  dbfs:/Volumes/zugzwang_dev/raw/landing/ \
  --recursive \
  --overwrite
```

> If your bundle's development schema names differ from `zugzwang_dev.raw`, use the exact Volume path produced by your current bundle deployment.

Verify the top-level remote layout:

```bash
databricks fs ls dbfs:/Volumes/zugzwang_dev/raw/landing/
```

Verify each source family:

```bash
databricks fs ls dbfs:/Volumes/zugzwang_dev/raw/landing/railway/
databricks fs ls dbfs:/Volumes/zugzwang_dev/raw/landing/stada/
databricks fs ls dbfs:/Volumes/zugzwang_dev/raw/landing/dwd/metadata/
```

For the larger DWD folders, count remote files:

```bash
databricks fs ls dbfs:/Volumes/zugzwang_dev/raw/landing/dwd/temperature/ | wc -l
databricks fs ls dbfs:/Volumes/zugzwang_dev/raw/landing/dwd/wind/ | wc -l
```

## Run the first Lakeflow validation and pipeline update

Validate the bundle:

```bash
databricks bundle validate -t dev
```

Deploy it:

```bash
databricks bundle deploy -t dev
```

Run a validation-only pipeline update first:

```bash
databricks bundle run -t dev --validate-only zugzwang_june2026
```

If validation succeeds, execute the pipeline:

```bash
databricks bundle run -t dev zugzwang_june2026
```

After the run, verify at minimum:

- `silver_train_stops` row count;
- unique railway EVA count;
- station join coverage;
- station-to-temperature mapping coverage;
- station-to-wind mapping coverage;
- temperature observation match rate;
- wind observation match rate;
- `gold_train_stop_weather` row count;
- no accidental row multiplication in the Gold join.

For the June 2026 railway source, the expected train-stop input count is:

```text
14,752,336
```

The Gold row count should remain equal to the train-stop row count if all enrichment joins preserve the stop-event grain.

## Source references

- Railway dataset: `https://huggingface.co/datasets/piebro/deutsche-bahn-data`
- DWD hourly temperature: `https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/`
- DWD hourly wind: `https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/recent/`

