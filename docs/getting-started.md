# 🏁 Getting Started

This guide walks you through setting up a fresh data engineering workspace using `dex` from scratch.

## 1. Project Scaffolding

Navigate to a clean directory where you want to start your data platform project and run the interactive initializer wizard via the CLI:

```bash
dex init

```

The wizard will prompt you for basic project metadata (Project Name, Version, Description, and Author) with smart fallbacks. Once completed, `dex` builds the following immutable convention-based layout:

```text
your-project/
├── conf/
│   └── catalog/
│       └── sample_dataset.yaml   # Declarative dataset definitions
├── data/
│   └── sample_table.csv         # A real physical boilerplate data source
├── src/
│   └── notebooks/               # Standardized zone for Jupyter research
└── pyproject.toml               # The root anchor of your data project

```

## 2. Declaring Datasets in the Catalog

Open `conf/catalog/sample_dataset.yaml`. Your datasets are declared as logical keys. You can specify the serialization format, the processing runtime engine, and its location path relative to the `pyproject.toml` file:

```yaml
sample_table:
  description: 'Boilerplate example dataset created by dex'
  format: 'csv'
  engine: 'pandas'
  storage_path: 'data/sample_table.csv'
  columns:
    - name: 'id'
    - name: 'name'

```

## 3. Loading Data in a Notebook

Create a new Jupyter Notebook inside `src/notebooks/`. You do not need to hardcode brittle backtracking string paths (`../../data/...`) or handle configuration parsers. Simply initialize the engine:

```python
from data_engineering_exp.core.io import DataLoader

# 1. Instantiate the loader (it automatically walks up to find pyproject.toml)
loader = DataLoader()

# 2. Fetch the dataset into memory using its logical identifier
df = loader.load("sample_table")

# 3. Inspect your data instantly
df.head()

```

## 4. Switching to PySpark seamlessly

If your dataset scales up and requires a distributed runtime, you do not need to rewrite your processing scripts. Simply add a Spark entry profile to your YAML file:

```yaml
sample_table_spark:
  format: 'csv'
  engine: 'spark'
  storage_path: 'data/sample_table.csv'

```

Then launch your active session inside your notebook. `dex` is smart enough to auto-detect the global context without manual injections:

```python
from pyspark.sql import SparkSession
from data_engineering_exp.core.io import DataLoader

# Initialize local Spark
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Load using the exact same interface
loader = DataLoader()
df_spark = loader.load("sample_table_spark")

df_spark.show()

```