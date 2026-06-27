"""This module implements an extensible, transactional project scaffolding engine for flint."""

import logging
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Union, runtime_checkable

from flint_core.core.exceptions import FlintError

logger = logging.getLogger(__name__)


class ProjectInitializationError(FlintError):
    """Raised when the project scaffolding pipeline encounters an unrecoverable failure."""

    pass


# =============================================================================
# SCAFFOLDING DOMAIN ENTITIES & CONTEXT
# =============================================================================


class ScaffoldContext:
    """High-performance, optimized state container tracking pipeline execution parameters."""

    __slots__ = ("root_path", "name", "version", "description", "author", "metadata", "created_paths")

    def __init__(
        self,
        root_path: Path,
        name: str,
        version: str,
        description: str,
        author: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initializes the operational contextual memory boundary.

        Args:
            root_path: System target directory path anchoring the new project workspace.
            name: Technical and presentation name of the data engineering project.
            version: Target semantic version string initialization placeholder.
            description: Narrative purpose statement explaining the data repository.
            author: Identity marker tracking the project creator.
            metadata: Extensible generic map tracking plug-in overrides parameters.
        """
        self.root_path: Path = root_path.resolve()
        self.name: str = name
        self.version: str = version
        self.description: str = description
        self.author: str = author
        self.metadata: Dict[str, Any] = metadata if metadata is not None else {}
        self.created_paths: List[Path] = []


@runtime_checkable
class ScaffoldStep(Protocol):
    """Structural protocol governing atomic, custom project initialization stages."""

    @property
    def name(self) -> str:
        """Returns the technical display identifier name of this pipeline stage."""
        ...

    def execute(self, context: ScaffoldContext) -> None:
        """Executes the specific scaffolding routine alterations on disk.

        Args:
            context: Active operational parameters proxy boundary container.
        """
        ...

    def rollback(self, context: ScaffoldContext) -> None:
        """Reverts local modifications safely if subsequent stages fail execution.

        Args:
            context: Active operational parameters proxy boundary container.
        """
        ...


# =============================================================================
# CONCRETE BASELINE FRAMEWORK STEPS
# =============================================================================


class DirectoryStructureStep:
    """Core stage orchestrating the transactional allocation of physical folders layouts."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Directory Structure Layout Scaffolding"

    def execute(self, context: ScaffoldContext) -> None:
        folders = [
            context.root_path / "conf" / "catalog",
            context.root_path / "src" / "notebooks",
            context.root_path / "data",
        ]
        for folder in folders:
            if not folder.exists():
                folder.mkdir(parents=True, exist_ok=True)
                context.created_paths.append(folder)
                logger.info("Created system directory convention: %s", folder.relative_to(context.root_path.parent))

    def rollback(self, context: ScaffoldContext) -> None:
        # Revert directories in inverted sequential execution structure layers
        for folder in reversed(context.created_paths):
            if folder.is_dir() and folder.exists() and not any(folder.iterdir()):
                folder.rmdir()
                logger.warning("Rollback operational cleanup: Removed empty directory %s", folder)


class PyProjectTomlStep:
    """Core stage orchestrating the safe generation of the project anchor framework parameters."""

    __slots__ = ()

    @property
    def name(self) -> str:
        return "Root Configuration Manifest Generation"

    def execute(self, context: ScaffoldContext) -> None:
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.exists():
            raise FileExistsError(
                f"A configuration manifest file already exists at target location: '{toml_path}'. "
                f"Aborting execution layout pipelines to prevent catastrophic data overwrites."
            )

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
        logger.info("Generated configuration layout file: %s", toml_path.name)

    def rollback(self, context: ScaffoldContext) -> None:
        toml_path = context.root_path / "pyproject.toml"
        if toml_path.is_file() and toml_path.exists():
            toml_path.unlink()
            logger.warning("Rollback operational cleanup: Evicted broken manifest asset: %s", toml_path)


class SampleDataStep:
    """Optional baseline step dropping an operational seed boilerplate table dataset asset."""

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
        logger.info("Generated baseline boilerplate sample table: data/%s", csv_path.name)

    def rollback(self, context: ScaffoldContext) -> None:
        csv_path = context.root_path / "data" / "sample_table.csv"
        if csv_path.is_file() and csv_path.exists():
            csv_path.unlink()


class SampleCatalogStep:
    """Core step linking the seed boilerplate matrix directly onto decentral catalog structures."""

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
        logger.info("Generated sample schema catalog mapping layout: conf/catalog/%s", catalog_path.name)

    def rollback(self, context: ScaffoldContext) -> None:
        catalog_path = context.root_path / "conf" / "catalog" / "sample_dataset.yaml"
        if catalog_path.is_file() and catalog_path.exists():
            catalog_path.unlink()


# =============================================================================
# CENTRAL INITIALIZATION ENGINE PIPELINE
# =============================================================================


class ProjectInitializer:
    """Enterprise-grade transactional engine orchestration project workspaces setups pipelines."""

    _pipeline: List[ScaffoldStep] = [
        DirectoryStructureStep(),
        PyProjectTomlStep(),
        SampleDataStep(),
        SampleCatalogStep(),
    ]

    def __init__(self, base_path: Union[str, Path]) -> None:
        """Initializes the coordinator mapping spatial targets boundaries hooks.

        Args:
            base_path: Destination root path location driving execution scopes.
        """
        self.base_path: Path = Path(base_path).resolve()

    @classmethod
    def register_scaffold_step(cls, step: ScaffoldStep) -> None:
        """Pluggable architectural entry-point to insert custom workflow steps seamlessly.

        Allows developers to extend scaffolding layouts (e.g., adding GitInitStep, DockerfileStep)
        without mutating core framework source components libraries.

        Args:
            step: Instance implementing the ScaffoldStep boundary contract protocol.
        """
        if not isinstance(step, ScaffoldStep):
            raise TypeError("Custom step insertion layout matches invalid specifications signatures protocol.")
        cls._pipeline.append(step)
        logger.info("Registered custom external scaffolding step layout extension: %s", step.name)

    def init_project(self, name: str, version: str, description: str, author: str) -> None:
        """Executes the pipeline loop within a rigid data transaction context.

        Args:
            name: Presentations system name workspace layout mapping flag string.
            version: Structural target package semantic identifier tracking code.
            description: Narrative contextual description logging repository value targets.
            author: Identity handle marking baseline creator structures allocations.

        Raises:
            ProjectInitializationError: If unexpected I/O block anomalies trigger rollback requirements.
        """
        context = ScaffoldContext(
            root_path=self.base_path, name=name, version=version, description=description, author=author
        )

        completed_stages: List[ScaffoldStep] = []
        logger.info("Triggering flint automated initialization pipeline matrix context tracker...")

        for step in self._pipeline:
            try:
                logger.debug("Executing transactional stage context element: %s", step.name)
                step.execute(context)
                completed_stages.append(step)
            except Exception as error:
                logger.error("Scaffolding pipeline broke at stage '%s' owing to: %s", step.name, error)
                self._abort_and_rollback(completed_stages, context)
                raise ProjectInitializationError(
                    f"Initialization transaction failed at runtime during step sequence '{step.name}': {error}"
                ) from error

        logger.info("Pipeline cluster initialization process finished safely for targeted workspace: '%s'.", name)

    def _abort_and_rollback(self, completed_stages: List[ScaffoldStep], context: ScaffoldContext) -> None:
        """Iterates backwards over finished stages to guarantee filesystem atomicity on failures."""
        logger.warning("Initiating emergency reverse cleanup transactional rolls back cycles pipelines...")
        for step in reversed(completed_stages):
            try:
                logger.debug("Reverting modifications executed by targeted entity block: %s", step.name)
                step.rollback(context)
            except Exception as rollback_error:
                logger.critical(
                    "Rollback routine failed for step '%s': %s. Structural state might be inconsistent.",
                    step.name,
                    rollback_error,
                )
