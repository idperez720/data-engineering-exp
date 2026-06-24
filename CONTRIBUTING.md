# Contributing to flint-core

Thank you for your interest in contributing to **flint-core**! We aim to build a highly versatile, minimalist, and robust "Swiss Army knife" framework for data engineers. 

To maintain high code quality and streamline our automated release pipeline, we enforce strict development standards. Please read through this guide before submitting any contributions.

---

## 🛠️ Local Development Setup

We use **Poetry** for dependency and environment management. To set up your local development environment:

1. **Fork and Clone** the repository.
2. Install all core and development dependencies:
   ```bash
   poetry install --with dev
    ```

3. Activate the virtual environment:
    ```bash
    poetry shell
    ```



---

## 📐 Code Quality & Testing Standards

Before submitting a Pull Request, your code must pass all local checks. Our CI/CD pipeline enforces these rules rigidly:

* **Code Linting & Formatting:** We use `Ruff`. The maximum line length is strictly capped at **88 characters**.
```bash
# Check for linting errors
poetry run ruff check .

# Verify code formatting compliance
poetry run ruff format --check .

```


* **Type Hints:** All function signatures, arguments, and return types must be fully explicitly typed. Avoid using `Any` wherever possible.
* **Docstrings:** Public APIs must contain structured English docstrings detailing `Args:`, `Returns:`, and `Raises:` clauses.
* **I/O Safety:** Always explicitly include `encoding="utf-8"` on all native Python file reads/writes (`open()`).
* **Unit Tests & Coverage:** Every new feature or bug fix must include corresponding tests under the `tests/` directory. Your changes **must not drop the overall repository test coverage below 60%**.
```bash
poetry run pytest --cov=flint_core --cov-fail-under=60

```



---

## 📝 Commit Message Standards (Crucial)

Our deployment pipeline is completely automated through **Semantic Releases**. The version numbers and changelogs are generated automatically based on your commit messages.

We strictly follow the **Conventional Commits** specification. Commit messages must use the following format:

```text
type(optional-scope): concise description in technical English

```

### Commit Types that Trigger a PyPI Release

| Commit Type | Purpose | Version Bump Impact | Example |
| --- | --- | --- | --- |
| `fix:` | Patches a bug or resolves an issue in the source code. | **Patch** (e.g., `0.1.0` $\rightarrow$ `0.1.1`) | `fix(io): resolve spark active session detection timeout` |
| `feat:` | Introduces a brand-new feature or capability. | **Minor** (e.g., `0.1.0` $\rightarrow$ `0.2.0`) | `feat: implement elastic dataframe metadata saving hook` |
| `!` *(Breaking)* | Appending a `!` to the type signifies a breaking change. | **Major** (e.g., `0.1.0` $\rightarrow$ `1.0.0`) | `feat!(catalog): overhaul yaml schema specification structure` |

### Commit Types that Do NOT Trigger a Release

The following prefixes will pass CI/CD pipeline tests but **will not** trigger a new version deploy to PyPI. Use them for routine development updates:

* `docs:` Documentation-only changes (e.g., `docs: update troubleshooting guide`).
* `style:` Formatting changes that do not affect the meaning of the code (e.g., fixing whitespace via Ruff).
* `refactor:` Code changes that neither fix a bug nor add a feature (e.g., optimizing an internal loop).
* `test:` Adding missing tests or correcting existing test suites.
* `chore:` Updating build tasks, local configurations, or Poetry dev dependencies.

---

## 🚀 The Pull Request Process

1. Create a descriptive branch for your work (e.g., `feat/add-json-parser` or `fix/catalog-path-resolver`).
2. Ensure all local tests and linter checks pass seamlessly.
3. Commit your changes utilizing the Conventional Commits standards defined above.
4. Push your branch to your origin fork and open a **Pull Request (PR)** against our `main` branch.
5. Our automated GitHub Actions pipeline will validate your PR. Once all automated checks turn green, a maintainer will review the code and merge it.

*Note: Upon a successful merge into `main`, the automated deployment layer will calculate the new semantic version, append the changes to the CHANGELOG, and instantly publish the new artifact to PyPI.*

