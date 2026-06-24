"""Unit tests for the Click-based flint Command Line Interface (CLI)."""

from pathlib import Path

from click.testing import CliRunner

from flint_core.cli import entry_point


def test_cli_init_command(tmp_path: Path) -> None:
    """Verifies that the Click CLI 'init' subcommand creates files successfully.

    Args:
        tmp_path(Path): Pytest built-in temporary directory factory fixture.

    Returns:
        None: Test assertions verify filesystem side-effects.
    """
    target_path = tmp_path / "my_interactive_project"
    runner = CliRunner()

    # Feeds 4 simulated Enter key strokes to automatically select default prompts
    simulated_inputs = "\n\n\n\n"

    result = runner.invoke(
        entry_point, ["init", "--path", str(target_path)], input=simulated_inputs
    )

    assert result.exit_code == 0
    assert "Project successfully initialized" in result.output

    expected_catalog_dir = target_path / "conf" / "catalog"
    expected_toml = target_path / "pyproject.toml"

    assert expected_catalog_dir.exists()
    assert expected_toml.exists()
