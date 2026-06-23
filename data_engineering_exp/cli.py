"""Command Line Interface (CLI) entry point for dex using Click."""

import os

import click

from data_engineering_exp.core.initializer import ProjectInitializer


@click.group()
def entry_point() -> None:
    """dex - Data Engineering Experience CLI utilities.

    Args:

    Returns:
        None: Main CLI entry group.
    """
    pass


@entry_point.command()
@click.option(
    "--path",
    type=click.Path(file_okay=False, dir_okay=True),
    default=".",
    help="Root directory where scaffolding will be built. Defaults to '.'",
)
def init(path: str) -> None:
    """Scaffold a new data engineering project structure interactively.

    Args:
        path(str): Target directory string path for project structure.

    Returns:
        None: Side-effect creating folders and configurations.
    """
    click.echo("🚀 Welcome to the dex project initialization wizard!\n")

    # Derives default project name dynamically from the target directory name
    default_name = os.path.basename(os.path.abspath(path))
    if not default_name or default_name in (".", ""):
        default_name = "my-dex-project"

    # Sequential metadata prompts with default fallbacks
    name = click.prompt("Project name", default=default_name, type=str)
    version = click.prompt("Version", default="0.1.0", type=str)
    description = click.prompt(
        "Description",
        default="Data engineering project scaffolded by dex",
        type=str,
    )
    author = click.prompt("Author", default="Anonymous", type=str)

    initializer = ProjectInitializer(base_path=path)
    initializer.init_project(
        name=name,
        version=version,
        description=description,
        author=author,
    )

    click.echo(f"\n✨ Project successfully initialized at '{path}'!")
