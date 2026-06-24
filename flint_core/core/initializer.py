"""This module implements the project structural initialization engine for flint."""

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
        name: str = "my-flint-project",
        version: str = "0.1.0",
        description: str = "Data engineering project scaffolded by flint",
        author: str = "Anonymous",
    ) -> None:
        """Scaffolds the required folders and creates baseline configuration templates.

        Args:
            name(str): Operational name of the data project. Defaults to
                "my-flint-project".
            version(str): Initial semver string version. Defaults to "0.1.0".
            description(str): Short purpose statement describing the repository.
                Defaults to "Data engineering project scaffolded by flint".
            author(str): Full name or alias identifier of the creator. Defaults
                to "Anonymous".

        Returns:
            None: Side-effect function creating disk structures.
        """
        folders = [
            os.path.join(self.base_path, "conf", "catalog"),
            os.path.join(self.base_path, "src", "notebooks"),
            os.path.join(self.base_path, "data"),
        ]

        for folder in folders:
            os.makedirs(folder, exist_ok=True)

        # Create a clean, standard pyproject.toml as the anchor of the project
        toml_path = os.path.join(self.base_path, "pyproject.toml")
        toml_content = (
            "[project]\n"
            f'name = "{name}"\n'
            f'version = "{version}"\n'
            f'description = "{description}"\n'
            "authors = [\n"
            f'    {{name = "{author}"}}\n'
            "]\n"
            'requires-python = ">=3.11,<4.0.0"\n'
        )
        with open(toml_path, "w", encoding="utf-8") as file:
            file.write(toml_content)

        # Drop a real physical sample CSV file into the new data folder
        csv_path = os.path.join(self.base_path, "data", "sample_table.csv")
        csv_content = "id,name\n1,Alice\n2,Bob\n"
        with open(csv_path, "w", encoding="utf-8") as csv_file:
            csv_file.write(csv_content)

        # Inject a sample boilerplate YAML catalog pointing to the real CSV
        sample_catalog_path = os.path.join(
            self.base_path, "conf", "catalog", "sample_dataset.yaml"
        )
        sample_content = (
            "sample_table:\n"
            "  description: 'Boilerplate example dataset created by flint'\n"
            "  format: 'csv'\n"
            "  engine: 'pandas'\n"
            "  storage_path: 'data/sample_table.csv'\n"
            "  columns:\n"
            "    - name: 'id'\n"
            "      type: 'integer'\n"
            "    - name: 'name'\n"
            "      type: 'string'\n"
        )
        with open(sample_catalog_path, "w", encoding="utf-8") as file:
            file.write(sample_content)
