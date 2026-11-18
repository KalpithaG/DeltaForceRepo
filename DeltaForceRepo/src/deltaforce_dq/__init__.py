"""Delta Force Data Quality & Monitoring Hub.

A config-driven, self-healing data reliability hub for Spark / Databricks.
Run everything with ``deltaforce_dq.pipeline.run_all(spark, settings)``.
"""

from .config import Settings  # noqa: F401

__version__ = "0.1.0"
