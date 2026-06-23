# Contributing to dex 🚀

First off, thank you for taking the time to contribute! Contributions are what make the open-source community such an amazing place to learn, inspire, and create. 

By participating in this project, you agree to abide by our coding standards and maintain a respectful, collaborative environment.

---

## 🗺️ How Can I Contribute?

### 1. Reporting Bugs 🐛
If you find a bug or unexpected behavior, please search the existing issues to ensure it hasn't been reported yet. If it's a new issue, open a bug report and include:
* A clear, descriptive title.
* Steps to reproduce the problem.
* The expected vs. actual behavior.
* Your environment details (Python version, OS, Pandas/PySpark versions).

### 2. Suggesting Enhancements 💡
We are always open to new ideas! If you want to propose a new feature or architectural change:
* Open a feature request issue.
* Explain the use case and why this change would benefit the framework's users.
* Describe your proposed solution or design considerations.

### 3. Code Contributions 🛠️
Ready to write some code? Here is our standard development workflow:

1. **Fork the Repository:** Create a personal fork of the project on GitHub.
2. **Clone your Fork:** Clone it locally and set up your development environment.
3. **Create a Branch:** Always base your work on the `dev` branch (or `main` if `dev` is not present) and name your branch descriptively:
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/bug-description

```

4. **Write Code and Tests:** Implement your changes and add corresponding unit tests under the `tests/` directory.
5. **Run Tests:** Ensure all existing and new tests pass perfectly before submitting:
```bash
poetry run pytest

```


6. **Submit a Pull Request (PR):** Open a PR targeting the `dev` branch of the original repository.

---

## 📐 Coding Standards & Guidelines

To maintain code quality and structural integrity, all contributions must strictly adhere to the following guardrails:

* **Line Length Limit:** Code formatting follows a strict **88-character maximum line length** enforced by **Ruff**.
* **Type Hints:** All functional methods, arguments, and return variables **must be explicitly type-hinted**. Avoid using plain `Any` unless strictly necessary for third-party compatibility layers.
* **Docstrings:** All public classes and functions must include descriptive English docstrings following our standardized structure:
```python
def look_up_root(path: str) -> str:
    """Brief one-line summary explaining the core action.

    Detailed context or implementation notes if applicable.

    Args:
        path(str): Description of the input target.

    Returns:
        str: Description of the return output.

    Raises:
        FileNotFoundError: Condition under which this exception triggers.
    """

```


* **Encoding:** Always specify the explicit `encoding="utf-8"` parameter when reading or writing text files.

---

## ⚖️ License Note

By contributing to `dex`, you agree that your contributions will be licensed under the project's **MIT License**. Your authorship will be recognized, but your code must safely carry the original liability waiver and copyright notice.
