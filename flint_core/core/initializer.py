"""This module implements the project structural initialization engine for flint."""

import logging
import textwrap
from pathlib import Path

# Initialize logger for the library
logger = logging.getLogger(__name__)


def initialize_project(
    target_path: Path,
    name: str = "my-flint-project",
    version: str = "0.1.0",
    description: str = "Data engineering project scaffolded by flint",
    author: str = "Anonymous",
    overwrite: bool = False,
) -> None:
    """Scaffolds the required folders and creates baseline configuration templates.

    Args:
        target_path: Root system directory where scaffolding will be built.
        name: Operational name of the data project.
        version: Initial semantic version string.
        description: Short purpose statement describing the repository.
        author: Full name or alias identifier of the creator.
        overwrite: If True, existing files will be overwritten. Defaults to False.

    Raises:
        FileExistsError: If a baseline file already exists and overwrite is False.
    """
    root = Path(target_path).resolve()

    # 1. Define targeted paths using Path objects
    folders = [
        root / "conf" / "catalog",
        root / "src" / "notebooks",
        root / "data",
    ]

    toml_path = root / "pyproject.toml"
    csv_path = root / "data" / "sample_table.csv"
    catalog_path = root / "conf" / "catalog" / "sample_dataset.yaml"

    # 2. Defensive Programming: Prevent accidental data loss
    if not overwrite and toml_path.exists():
        raise FileExistsError(
            f"A project configuration already exists at '{toml_path}'. "
            f"Use 'overwrite=True' if you intend to recreate the scaffold."
        )

    logger.info("Initializing structural layout at %s", root)

    # 3. Create directories safely
    for folder in folders:
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
            logger.info("Created directory: %s", folder.relative_to(root.parent))

    # 4. Generate pyproject.toml using textwrap.dedent for clean spacing
    toml_content = textwrap.dedent(f"""\
        [project]
        name = "{name}"
        version = "{version}"
        description = "{description}"
        authors = [
            {{name = "{author}"}}
        ]
        requires-python = ">=3.11,<4.0.0"
    """)

    with open(toml_path, "w", encoding="utf-8") as file:
        file.write(toml_content)
    logger.info("Generated configuration file: %s", toml_path.name)

    # 5. Drop standard sample CSV file
    csv_content = textwrap.dedent("""\
        id,name
        1,Alice
        2,Bob
    """)
    with open(csv_path, "w", encoding="utf-8") as csv_file:
        csv_file.write(csv_content)
    logger.info("Generated sample data: data/%s", csv_path.name)

    # 6. Inject boilerplate YAML catalog pointing to the CSV
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
    logger.info("Generated sample catalog mapping: conf/catalog/%s", catalog_path.name)

    logger.info("Project '%s' successfully initialized.", name)
