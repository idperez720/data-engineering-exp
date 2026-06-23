# 🚀 Welcome to dex

**dex** (*Data Engineering Experience*) is a minimalist, agnostic framework designed to standardize data engineering pipelines in Python. It completely eliminates environmental friction, Spark session management boilerplates, and fragile absolute path hardcoding.

## ✨ Core Features

* **Convention over Configuration:** Say goodbye to cluttered, redundant config blocks. Structure your workspace once, and let `dex` handle the orchestration safely.
* **Decentralized Data Catalog:** Declare your datasets using distributed, self-contained mini-YAML files inside your repository.
* **Elastic Data Loading:** Load data using **Pandas** or **PySpark** using the exact same unified interface. `dex` dynamically resolves absolute filesystem boundaries and hooks into active Spark contexts behind the scenes.

## 📦 Quick Showcase

```python
from data_engineering_exp.core.io import DataLoader

# Automatically discovers the project root via the pyproject.toml anchor
loader = DataLoader()

# Elastic execution: loads the dataset using the optimal engine stated in YAML
df = loader.load("sample_table")
```