"""
Flint Core: A minimalist, agnostic Python framework to standardize data engineering
pipelines.
"""

import logging

__version__ = "0.1.0"

logger = logging.getLogger(__name__)

# Safely trigger engine registration via metaprogramming upon package initialization.
# This ensures that if the user installed flint-core[pandas], the PandasEngine is loaded
# without forcing the SparkEngine to trigger an unexpected ImportError.

try:
    # Assuming PandasEngine or associated components live inside or are exposed by
    # pandas_core
    import flint_core.pandas_core  # noqa: F401

    logger.debug("Successfully initialized flint-core pandas backend.")
except ImportError:
    logger.debug(
        "Pandas backend optional dependencies not found. Skipping auto-registration."
    )

try:
    # Assuming SparkEngine or associated components live inside or are exposed by
    # spark_core
    import flint_core.spark_core  # noqa: F401

    logger.debug("Successfully initialized flint-core spark backend.")
except ImportError:
    logger.debug(
        "Spark backend optional dependencies not found. Skipping auto-registration."
    )
