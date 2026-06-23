"""Unit tests for the ProjectInitializer core scaffolding layer."""

from pathlib import Path

from data_engineering_exp.core.initializer import ProjectInitializer


def test_project_initializer_scaffolding(tmp_path: Path) -> None:
    """Verifies that initializer creates directories and default files safely.

    Args:
        tmp_path(Path): Pytest built-in temporary directory factory fixture.

    Returns:
        None: Test assertions verify filesystem side-effects.
    """
    initializer = ProjectInitializer(base_path=str(tmp_path))
    initializer.init_project(
        name="custom-repo",
        version="2.4.1",
        description="Data platform foundation",
        author="Data Team",
    )

    expected_toml = tmp_path / "pyproject.toml"
    assert expected_toml.exists()

    toml_content = expected_toml.read_text(encoding="utf-8")
    assert 'name = "custom-repo"' in toml_content
    assert 'version = "2.4.1"' in toml_content
    assert 'name = "Data Team"' in toml_content