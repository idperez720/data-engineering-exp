"""This module implements the project structural initialization engine for dex."""

import os


class ProjectInitializer:
    """Handles scaffolding of directory structures for new data engineering setups.

    Automates the creation of standard configuration folders and source directories
    to establish a unified project layout.
    """

    def __init__(self, base_path: str = ".") -> None:
        """Initializes the ProjectInitializer with a target base path.

        Args:
            base_path(str): Root system path where scaffolding will be built.
                Defaults to ".".

        Returns:
            None: Initializes the class instance.
        """
        self.base_path = base_path

    def init_project(
        self,
        name: str = "my-dex-project",
        version: str = "0.1.0",
        description: str = "Data engineering project scaffolded by dex",
        author: str = "Anonymous",
    ) -> None:
        """Scaffolds the required folders and creates baseline configuration templates.

        Args:
            name(str): Operational name of the data project. Defaults to
                "my-dex-project".
            version(str): Initial semver string version. Defaults to "0.1.0".
            description(str): Short purpose statement describing the repository.
                Defaults to "Data engineering project scaffolded by dex".
            author(str): Full name or alias identifier of the creator. Defaults
                to "Anonymous".

        Returns:
            None: Side-effect function creating disk structures.
        """
        folders = [
            os.path.join(self.base_path, "conf", "catalog"),
            os.path.join(self.base_path, "src", "notebooks"),
        ]

        for folder in folders:
            os.makedirs(folder, exist_ok=True)

        # Build custom localized configuration file using interactive properties
        toml_path = os.path.join(self.base_path, "pyproject.toml")
        toml_content = (
            "[project]\n"
            f'name = "{name}"\n'
            f'version = "{version}"\n'
            f'description = "{description}"\n'
            "authors = [\n"
            f'    {{name = "{author}"}}\n'
            "]\n"
            'requires-python = ">=3.11"\n'
        )
        with open(toml_path, "w", encoding="utf-8") as file:
            file.write(toml_content)

        # Inject a sample boilerplate YAML catalog to get the user started
        sample_catalog_path = os.path.join(
            self.base_path, "conf", "catalog", "sample_dataset.yaml"
        )
        sample_content = (
            "sample_table:\n"
            "  description: 'Boilerplate example dataset created by dex'\n"
            "  format: 'parquet'\n"
            "  columns:\n"
            "    - name: 'id'\n"
            "      type: 'integer'\n"
            "    - name: 'name'\n"
            "      type: 'string'\n"
        )
        with open(sample_catalog_path, "w", encoding="utf-8") as file:
            file.write(sample_content)