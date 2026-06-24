"""Unit tests for the ProjectInitializer core scaffolding layer."""

from pathlib import Path

from flint_core.core.initializer import ProjectInitializer


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
    expected_csv = tmp_path / "data" / "sample_table.csv"
    expected_yaml = tmp_path / "conf" / "catalog" / "sample_dataset.yaml"

    assert expected_toml.exists()
    assert expected_csv.exists()
    assert expected_yaml.exists()

    # Verify CSV file contents
    csv_content = expected_csv.read_text(encoding="utf-8")
    assert "id,name" in csv_content
    assert "Alice" in csv_content

    # Verify YAML configuration attributes
    yaml_content = expected_yaml.read_text(encoding="utf-8")
    assert "format: 'csv'" in yaml_content
    assert "storage_path: 'data/sample_table.csv'" in yaml_content
