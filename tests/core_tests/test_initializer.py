"""Unit tests for the flint-core transactional project scaffolding engine."""

from pathlib import Path

import pytest

from flint_core.core.initializer import (
    ProjectInitializationError,
    ProjectInitializer,
    ScaffoldContext,
)


def test_project_initializer_happy_path(tmp_path: Path) -> None:
    """Asserts that a standard project initialization seeds all assets."""
    initializer = ProjectInitializer(base_path=tmp_path)

    initializer.init_project(
        name="enterprise-pipeline",
        version="1.0.0",
        description="Core financial ledger ingestion architecture.",
        author="Architecture Team",
    )

    # 1. Verify directory structural layout
    assert (tmp_path / "conf" / "catalog").is_dir()
    assert (tmp_path / "src" / "notebooks").is_dir()
    assert (tmp_path / "data").is_dir()

    # 2. Verify pyproject.toml manifest content
    toml_path = tmp_path / "pyproject.toml"
    assert toml_path.is_file()
    toml_content = toml_path.read_text(encoding="utf-8")
    assert 'name = "enterprise-pipeline"' in toml_content
    assert 'version = "1.0.0"' in toml_content
    assert 'name = "Architecture Team"' in toml_content

    # 3. Verify boilerplate data assets
    assert (tmp_path / "data" / "sample_table.csv").is_file()
    assert (tmp_path / "conf" / "catalog" / "sample_dataset.yaml").is_file()

    # 4. Verify Phase 1 global configuration convention file
    spark_path = tmp_path / "conf" / "spark.yml"
    assert spark_path.is_file()
    spark_content = spark_path.read_text(encoding="utf-8")
    assert "spark.sql.shuffle.partitions" in spark_content


def test_project_initializer_collision_raises_exception(tmp_path: Path) -> None:
    """Asserts that initializing over an existing manifest safely aborts."""
    existing_toml = tmp_path / "pyproject.toml"
    existing_toml.write_text("[project]\nname = 'dont-overwrite-me'", encoding="utf-8")

    initializer = ProjectInitializer(base_path=tmp_path)

    with pytest.raises(ProjectInitializationError) as exc_info:
        initializer.init_project(
            name="collision-test",
            version="0.1.0",
            description="Should fail",
            author="Tester",
        )

    assert "scaffolding transaction failed" in str(exc_info.value)
    assert "dont-overwrite-me" in existing_toml.read_text(encoding="utf-8")


def test_project_initializer_custom_step_registration(tmp_path: Path) -> None:
    """Validates framework capabilities to inject custom workflow steps."""

    class MockGitInitStep:
        @property
        def name(self) -> str:
            return "Mock Git Repository Initialization"

        def execute(self, context: ScaffoldContext) -> None:
            git_dir = context.root_path / ".git"
            git_dir.mkdir()
            context.created_paths.append(git_dir)

        def rollback(self, context: ScaffoldContext) -> None:
            git_dir = context.root_path / ".git"
            if git_dir.exists():
                git_dir.rmdir()

    original_pipeline = ProjectInitializer._pipeline[:]

    try:
        ProjectInitializer.register_scaffold_step(MockGitInitStep())
        initializer = ProjectInitializer(base_path=tmp_path)

        initializer.init_project(
            name="custom-step-test",
            version="0.1.0",
            description="Testing IoC hooks",
            author="Tester",
        )

        assert (tmp_path / ".git").is_dir()
        assert (tmp_path / "pyproject.toml").is_file()

    finally:
        ProjectInitializer._pipeline = original_pipeline


def test_project_initializer_transactional_rollback(tmp_path: Path) -> None:
    """Asserts atomicity: failures trigger full reverse cleanups cleanly."""

    class CatastrophicFaultyStep:
        @property
        def name(self) -> str:
            return "Simulated Network/Disk Failure Hook"

        def execute(self, context: ScaffoldContext) -> None:
            raise IOError("No space left on device or permission denied.")

        def rollback(self, context: ScaffoldContext) -> None:
            pass

    original_pipeline = ProjectInitializer._pipeline[:]

    try:
        ProjectInitializer.register_scaffold_step(CatastrophicFaultyStep())
        initializer = ProjectInitializer(base_path=tmp_path)

        with pytest.raises(ProjectInitializationError):
            initializer.init_project(
                name="broken-transaction",
                version="1.0.0",
                description="Rollback verification",
                author="Tester",
            )

        # The directory must be completely pristine, including spark.yml removal
        remaining_files = [p for p in tmp_path.rglob("*") if p != tmp_path]
        assert len(remaining_files) == 0

    finally:
        ProjectInitializer._pipeline = original_pipeline
