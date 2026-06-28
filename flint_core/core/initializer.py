"""This module implements transactional project scaffolding engines for flint."""

import logging
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Union, runtime_checkable

from flint_core.core.exceptions import ProjectInitializationError

logger = logging.getLogger(__name__)


class ScaffoldContext:
    """High-performance container tracking pipeline execution parameters."""

    __slots__ = (
        "root_path",
        "name",
        "version",
        "description",
        "author",
        "metadata",
        "created_paths",
    )

    def __init__(
        self,
        root_path: Path,
        name: str,
        version: str,
        description: str,
        author: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.root_path: Path = root_path.resolve()
        self.name: str = name
        self.version: str = version
        self.description: str = description
        self.author: str = author
        self.metadata: Dict[str, Any] = metadata if metadata is not None else {}
        self.created_paths: List[Path] = []


@runtime_checkable
class ScaffoldStep(Protocol):
    """Structural protocol governing atomic project initialization stages."""

    @property
    def name(self) -> str: ...

    def execute(self, context: ScaffoldContext) -> None: ...

    def rollback(self, context: ScaffoldContext) -> None: ...


class DirectoryStructureStep:
    """Core stage orchestrating allocation of physical folders layouts."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Directory Structure Layout Scaffolding"

    def execute(self, context: ScaffoldContext) -> None:
        target_folders = [
            context.root_path / "conf" / "catalog",
            context.root_path / "src" / "notebooks",
            context.root_path / "data",
        ]
        for folder in target_folders:
            parts_to_create = []
            current = folder
            while current != context.root_path and current != current.parent:
                if not current.exists():
                    parts_to_create.append(current)
                    current = current.parent
                else:
                    break
            for part in reversed(parts_to_create):
                part.mkdir(exist_ok=True)
                context.created_paths.append(part)

    def rollback(self, context: ScaffoldContext) -> None:
        for item in reversed(context.created_paths):
            if item.is_dir() and item.exists() and not any(item.iterdir()):
                item.rmdir()


class PyProjectTomlStep:
    """Core stage orchestrating safe generation of project environment anchors."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Root Configuration Manifest Generation"

    def execute(self, context: ScaffoldContext) -> None:
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.exists():
            raise FileExistsError("A configuration manifest file already exists at target location.")
        toml_content = textwrap.dedent(f"""\
            [project]
            name = "{context.name}"
            version = "{context.version}"
            description = "{context.description}"
            authors = [
                {{name = "{context.author}"}}
            ]
            requires-python = ">=3.11,<4.0.0"
        """)
        with open(toml_path, "w", encoding="utf-8") as file:
            file.write(toml_content)
        context.created_paths.append(toml_path)

    def rollback(self, context: ScaffoldContext) -> None:
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.is_file() and toml_path.exists():
            toml_path.unlink()


class SampleDataStep:
    """Optional baseline step dropping an operational seed table asset."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Boilerplate Seed Sample Physical Data Insertion"

    def execute(self, context: ScaffoldContext) -> None:
        csv_path = context.root_path / "data" / "sample_table.csv"
        csv_content = textwrap.dedent("""\
            id,name
            1,Alice
            2,Bob
        """)
        with open(csv_path, "w", encoding="utf-8") as csv_file:
            csv_file.write(csv_content)
        context.created_paths.append(csv_path)

    def rollback(self, context: ScaffoldContext) -> None:
        csv_path = context.root_path / "data" / "sample_table.csv"
        if csv_path.is_file() and csv_path.exists():
            csv_path.unlink()


class SampleCatalogStep:
    """Core step linking the seed boilerplate onto decentral catalog structures."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Seed Declarative Catalog Configuration Generation"

    def execute(self, context: ScaffoldContext) -> None:
        catalog_path = context.root_path / "conf" / "catalog" / "sample_dataset.yaml"
        sample_catalog_content = textwrap.dedent("""\
            sample_table:
              description: 'Boilerplate example dataset created by flint'
              format: 'csv'
              engine: 'pandas'
              storage_path: 'data/sample_table.csv'
              columns:
                - name: 'id'
                  type: 'integer'
                - name: 'name'
                  type: 'string'
        """)
        with open(catalog_path, "w", encoding="utf-8") as file:
            file.write(sample_catalog_content)
        context.created_paths.append(catalog_path)

    def rollback(self, context: ScaffoldContext) -> None:
        catalog_path = context.root_path / "conf" / "catalog" / "sample_dataset.yaml"
        if catalog_path.is_file() and catalog_path.exists():
            catalog_path.unlink()


class SampleSparkConfigStep:
    """Core step generating a baseline template for global cloud runtimes."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Seed Spark Configuration Template Generation"

    def execute(self, context: ScaffoldContext) -> None:
        spark_path = context.root_path / "conf" / "spark.yml"
        spark_content = textwrap.dedent("""\
            # Global Spark configurations managed by convention via flint-core
            spark.sql.shuffle.partitions: "2"
            spark.default.parallelism: "2"
            spark.sql.execution.arrow.pyspark.enabled: "true"
        """)
        with open(spark_path, "w", encoding="utf-8") as file:
            file.write(spark_content)
        context.created_paths.append(spark_path)

    def rollback(self, context: ScaffoldContext) -> None:
        spark_path = context.root_path / "conf" / "spark.yml"
        if spark_path.is_file() and spark_path.exists():
            spark_path.unlink()


class ProjectInitializer:
    """Orchestrates modular pipeline stages under rigid transaction blocks."""

    _pipeline: List[ScaffoldStep] = [
        DirectoryStructureStep(),
        PyProjectTomlStep(),
        SampleDataStep(),
        SampleCatalogStep(),
        SampleSparkConfigStep(),
    ]

    def __init__(self, base_path: Union[str, Path]) -> None:
        self.base_path: Path = Path(base_path).resolve()

    @classmethod
    def register_scaffold_step(cls, step: ScaffoldStep) -> None:
        if not isinstance(step, ScaffoldStep):
            raise TypeError("Step matches invalid protocol specifications.")
        cls._pipeline.append(step)

    def init_project(self, name: str, version: str, description: str, author: str) -> None:
        context = ScaffoldContext(
            root_path=self.base_path,
            name=name,
            version=version,
            description=description,
            author=author,
        )
        completed_stages: List[ScaffoldStep] = []
        for step in self._pipeline:
            try:
                step.execute(context)
                completed_stages.append(step)
            except Exception as error:
                self._abort_and_rollback(completed_stages, context)
                raise ProjectInitializationError(f"scaffolding transaction failed at '{step.name}': {error}") from error

    def _abort_and_rollback(self, completed_stages: List[ScaffoldStep], context: ScaffoldContext) -> None:
        for step in reversed(completed_stages):
            try:
                step.rollback(context)
            except Exception as rollback_error:
                logger.critical("Rollback routine failed: %s", rollback_error)
