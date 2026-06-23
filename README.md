# Data Engineering Experience 🚀

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.14-blue)](https://www.python.org)

**dex** (*Data Engineering Experience*) is a minimalist, agnostic Python framework designed to streamline and standardize data engineering pipelines. By embracing **Convention over Configuration**, `dex` eliminates environment friction, absolute path hardcoding, and complex PySpark session management.

---

## ✨ Key Features

* **Zero-Config File Discovery:** Automatic tree-walking directory resolution anchors your data catalog using your local `pyproject.toml` file.
* **Decentralized Catalog:** Declare your metadata layouts inside modular, self-contained mini-YAML files.
* **Elastic Processing Runtimes:** Switch dynamically between **Pandas** and **PySpark** execution engines using exactly the same unified interface.
* **Interactive CLI Scaffolding:** Spin up a new production-ready data directory structure instantly with `dex init`.

---

## 📦 Installation

*(Once published to PyPI)*
```bash
pip install dex
```
Or install it directly from the source repository using Poetry:
```bash
poetry add git+[https://github.com/idperez720/data-engineering-exp.git](https://github.com/idperez720/data-engineering-exp.git)
```

---

## 🏁 Quick Start

### 1. Initialize your workspace

Navigate to an empty directory and let the interactive wizard scaffold the workspace conventions:

```bash
dex init

```

### 2. Declare a dataset

Add a specification block inside `conf/catalog/sample_dataset.yaml`:

```yaml
customers:
  description: "Main production customer data"
  format: "csv"
  engine: "pandas"
  storage_path: "data/sample_table.csv"

```

### 3. Load data anywhere

Create a Python script or open a Jupyter Notebook inside `src/notebooks/` and fetch your data instantly:

```python
from data_engineering_exp.core.io import DataLoader

# Autodiscovers your project root boundaries and settings
loader = DataLoader()

# Loads the dataset securely as a Pandas DataFrame
df = loader.load("customers")
df.head()

```

---

## 📖 Complete Documentation

For comprehensive guides, testing architecture deep-dives, and complete API references, visit our documentation site:
👉 **[http://127.0.0.1:8000/](https://www.google.com/search?q=http://127.0.0.1:8000/)** *(Replace with your deployed docs URL, e.g., GitHub Pages)*

---

## ⚖️ License

Distributed under the **MIT License**. Any modification or distribution (including forks) must include the original copyright notice and liability waiver. See `LICENSE` for more information.

```