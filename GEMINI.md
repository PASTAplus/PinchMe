# Project Context: PinchMe
**PinchMe** is an EDI (Electronic Data Interchange) data repository integrity scanner.

## 1. Core Environment & Constraints
- **Package Manager:** Pixi (Conda-based). Never use `pip` or `conda` directly.
- **Python Version:** 3.13.x (Strictly enforced).
- **Shell:** Fish Shell. All terminal commands must use Fish syntax (e.g., `set -x` instead of `export`).
- **Linter/Formatter:** Ruff (configured for Python 3.13 compatibility).

## 2. Mandatory Workflow (Pixi Tasks)
Before considering a task "done," you must execute the following via `pixi run`:
- `pixi run format`: To apply Ruff formatting.
- `pixi run check`: To perform linting and automatic fixes.
- `pixi run test`: To verify logic in the `tests/` directory.
- `pixi run run`: To test the `pinchme` CLI entry point.

## 3. Python Development Standards
- **Version:** Target **Python 3.13+** (specifically `3.13` as per pixi config).
- **Typing:** Strict type hints are mandatory for all function signatures. Use modern pipe syntax for unions (e.g., `str | None`).
- **Naming:** `snake_case` for variables/functions, `PascalCase` for classes.

## 4. Testing & Validation
- **Test Runner:** Use `pytest` via `pixi run test`.
- **Workflow:** 1. For bug fixes, create a reproduction test in `tests/` first.
  2. Run `pixi run check` and `pixi run test` before declaring a task "done."
- **CLI Testing:** The main entry point is `pinchme`. Test changes using `pixi run run`.
- 
## 5. Git Workflow
- **Git** Never perform a git pull, add, or commit - leave this to the developer.
- **GitHub** Never push to GitHub directly - leave this to the developer.
- Commit messages should use the following structure:
    - "Summary:" - A brief description of the commit.
    - "Context:" - The purpose or background of the commit.
    - "Changes:" - The specific changes made to the codebase.