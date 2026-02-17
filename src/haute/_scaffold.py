"""Scaffold templates for ``haute init``.

Each function returns a string for a specific file, parameterised by
project name, deploy target, and CI provider.
"""

from __future__ import annotations

# ── haute.toml ────────────────────────────────────────────────────────


def haute_toml(name: str, target: str, ci: str) -> str:
    """Generate ``haute.toml`` with only the relevant target section."""
    min_approvers = 2

    sections = [
        f"""\
[project]
name = "{name}"
pipeline = "main.py"

[deploy]
target = "{target}"
model_name = "{name}"
endpoint_name = "{name}"
""",
        _target_section(name, target),
        f"""\
[test_quotes]
dir = "tests/quotes"

[safety]
impact_dataset = "data/portfolio_sample.parquet"

[safety.approval]
min_approvers = {min_approvers}

[ci]
provider = "{ci}"

[ci.staging]
endpoint_suffix = "-staging"
""",
    ]
    return "\n".join(sections)


def _target_section(name: str, target: str) -> str:
    if target == "databricks":
        return f"""\
[deploy.databricks]
experiment_name = "/Shared/haute/{name}"
catalog = "main"
schema = "pricing"
serving_workload_size = "Small"
serving_scale_to_zero = true
"""
    if target == "docker":
        return """\
[deploy.docker]
registry = ""
port = 8080
base_image = "python:3.11-slim"
"""
    if target == "sagemaker":
        return """\
[deploy.sagemaker]
region = "eu-west-1"
instance_type = "ml.m5.large"
initial_instance_count = 1
"""
    if target == "azure-ml":
        return """\
[deploy.azure-ml]
resource_group = ""
workspace_name = ""
instance_type = "Standard_DS3_v2"
instance_count = 1
"""
    msg = f"Unknown target: {target}"
    raise ValueError(msg)


# ── .env.example ──────────────────────────────────────────────────────


def env_example(target: str) -> str:
    """Generate ``.env.example`` with only the credentials for the chosen target."""
    header = """\
# Haute - {label} credentials
# Copy this file to .env and fill in your values.
# .env is gitignored and will never be committed.
#
#   cp .env.example .env
"""
    if target == "databricks":
        return (
            header.format(label="Databricks")
            + """
DATABRICKS_HOST=https://adb-1234567890123456.12.azuredatabricks.net
DATABRICKS_TOKEN=your_databricks_token_here
"""
        )
    if target == "sagemaker":
        return (
            header.format(label="AWS SageMaker")
            + """
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=eu-west-1
SAGEMAKER_ROLE_ARN=arn:aws:iam::123456789012:role/SageMakerRole
"""
        )
    if target == "azure-ml":
        return (
            header.format(label="Azure ML")
            + """
AZURE_SUBSCRIPTION_ID=
AZURE_TENANT_ID=
AZURE_CLIENT_ID=
AZURE_CLIENT_SECRET=
"""
        )
    if target == "docker":
        return (
            header.format(label="Docker registry (optional)")
            + """
DOCKER_USERNAME=
DOCKER_PASSWORD=
"""
        )
    msg = f"Unknown target: {target}"
    raise ValueError(msg)


# ── GitHub Actions: ci.yml ────────────────────────────────────────────


def github_ci_yml() -> str:
    """PR checks workflow for GitHub Actions."""
    return """\
name: CI

on:
  pull_request:
    branches: [main]

permissions:
  contents: read
  pull-requests: write

jobs:
  lint:
    name: Lint & Format
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          python-version: "3.11"
      - run: uv sync --frozen
      - run: uv run ruff check .
      - run: uv run ruff format --check .

  typecheck:
    name: Type Check
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          python-version: "3.11"
      - run: uv sync --frozen
      - run: uv run mypy .

  test:
    name: Test
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          python-version: "3.11"
      - run: uv sync --frozen
      - run: uv run pytest -v

  pipeline-validate:
    name: Pipeline Validation
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          python-version: "3.11"
      - run: uv sync --frozen
      - name: Lint pipeline
        run: uv run haute lint
      - name: Dry-run deploy (score test quotes)
        run: uv run haute deploy --dry-run
"""


def github_deploy_yml(target: str) -> str:
    """Merge-to-main deploy workflow for GitHub Actions.

    Runs automatically: validate → staging → smoke test → impact analysis.
    Production is a separate manual workflow (``deploy-production.yml``)
    so it works on GitHub Free without environment protection rules.

    The impact-analysis job outputs the deployed git SHA so the
    production workflow can verify it is deploying exactly what was tested.
    """
    secrets_env = _github_secrets_env(target)

    return f"""\
name: Deploy

on:
  push:
    branches: [main]
    paths:
      - "*.py"
      - "haute.toml"
      - "data/**"
      - "models/**"
      - "tests/quotes/**"
  workflow_dispatch:

concurrency:
  group: deploy
  cancel-in-progress: false

jobs:
  validate:
    name: Validate
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          python-version: "3.11"
      - run: uv sync --frozen
      - name: Lint
        run: uv run ruff check .
      - name: Type check
        run: uv run mypy .
      - name: Test
        run: uv run pytest -v
      - name: Pipeline lint
        run: uv run haute lint
      - name: Dry-run deploy
        run: uv run haute deploy --dry-run

  deploy-staging:
    name: Deploy → Staging
    needs: validate
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          python-version: "3.11"
      - run: uv sync --frozen
      - name: Deploy to staging
        env:
{secrets_env}
        run: uv run haute deploy --endpoint-suffix "-staging"

  smoke-test:
    name: Smoke Test Staging
    needs: deploy-staging
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          python-version: "3.11"
      - run: uv sync --frozen
      - name: Score test quotes against staging endpoint
        env:
{secrets_env}
        run: uv run haute smoke --endpoint-suffix "-staging"

  impact-analysis:
    name: Impact Analysis
    needs: smoke-test
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          python-version: "3.11"
      - run: uv sync --frozen
      - name: Compare staging vs production predictions
        env:
{secrets_env}
        run: uv run haute impact --endpoint-suffix "-staging"
      - name: Upload impact report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: impact-report
          path: impact_report.md
      - name: Record deployed SHA
        run: echo "Staged commit: $GITHUB_SHA" >> "$GITHUB_STEP_SUMMARY"
"""


def github_deploy_prod_yml(target: str) -> str:
    """Manual production deploy workflow for GitHub Actions.

    Triggered via the GitHub Actions UI (workflow_dispatch) after
    reviewing the impact report from the deploy workflow.
    Works on GitHub Free — no environment protection rules needed.

    Accepts a ``sha`` input so the deployer can pin the exact commit
    that was staged and impact-analysed.  The workflow verifies that the
    checked-out HEAD matches, preventing accidental deployment of
    untested code.
    """
    secrets_env = _github_secrets_env(target)

    return f"""\
name: Deploy → Production

on:
  workflow_dispatch:
    inputs:
      sha:
        description: >
          Git SHA that was staged & impact-analysed.
          Leave blank to deploy current HEAD of main (less safe).
        required: false
        type: string

concurrency:
  group: deploy
  cancel-in-progress: false

permissions:
  contents: write

jobs:
  deploy-production:
    name: Deploy → Production
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - name: Verify commit matches staged SHA
        if: inputs.sha != ''
        run: |
          if [ "$GITHUB_SHA" != "${{{{ inputs.sha }}}}" ]; then
            echo "::error::HEAD ($GITHUB_SHA) does not match" \\
              "staged SHA (${{{{ inputs.sha }}}}). Merge may have occurred since staging."
            exit 1
          fi
      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true
          python-version: "3.11"
      - run: uv sync --frozen
      - name: Deploy to production
        env:
{secrets_env}
        run: uv run haute deploy
      - name: Tag release
        run: |
          set -euo pipefail
          VERSION=$(uv run haute status --version-only 2>/dev/null || echo "unknown")
          git tag "deploy/v$VERSION"
          git push origin "deploy/v$VERSION"
"""


def _github_secrets_env(target: str) -> str:
    """Return the env: block for GitHub Actions secrets, indented for YAML."""
    indent = "          "
    if target == "databricks":
        return (
            f"{indent}DATABRICKS_HOST: ${{{{ secrets.DATABRICKS_HOST }}}}\n"
            f"{indent}DATABRICKS_TOKEN: ${{{{ secrets.DATABRICKS_TOKEN }}}}"
        )
    if target == "sagemaker":
        return (
            f"{indent}AWS_ACCESS_KEY_ID: ${{{{ secrets.AWS_ACCESS_KEY_ID }}}}\n"
            f"{indent}AWS_SECRET_ACCESS_KEY: ${{{{ secrets.AWS_SECRET_ACCESS_KEY }}}}\n"
            f"{indent}AWS_DEFAULT_REGION: ${{{{ secrets.AWS_DEFAULT_REGION }}}}\n"
            f"{indent}SAGEMAKER_ROLE_ARN: ${{{{ secrets.SAGEMAKER_ROLE_ARN }}}}"
        )
    if target == "azure-ml":
        return (
            f"{indent}AZURE_SUBSCRIPTION_ID: ${{{{ secrets.AZURE_SUBSCRIPTION_ID }}}}\n"
            f"{indent}AZURE_TENANT_ID: ${{{{ secrets.AZURE_TENANT_ID }}}}\n"
            f"{indent}AZURE_CLIENT_ID: ${{{{ secrets.AZURE_CLIENT_ID }}}}\n"
            f"{indent}AZURE_CLIENT_SECRET: ${{{{ secrets.AZURE_CLIENT_SECRET }}}}"
        )
    if target == "docker":
        return (
            f"{indent}DOCKER_USERNAME: ${{{{ secrets.DOCKER_USERNAME }}}}\n"
            f"{indent}DOCKER_PASSWORD: ${{{{ secrets.DOCKER_PASSWORD }}}}"
        )
    msg = f"Unknown target: {target}"
    raise ValueError(msg)


# ── GitLab CI ────────────────────────────────────────────────────────


def gitlab_ci_yml(target: str) -> str:
    """Combined CI + deploy pipeline for GitLab CI/CD.

    Uses GitLab stages to enforce ordering.  The ``deploy-production``
    job uses ``when: manual`` so a reviewer must click to approve.

    Credentials are only available in deploy/smoke/impact/production
    jobs (protected-branch jobs), not in the MR validation job.
    """
    secrets_env = _gitlab_secrets_env(target)

    return f"""\
stages:
  - validate
  - deploy-staging
  - smoke-test
  - impact-analysis
  - deploy-production

default:
  image: python:3.11
  cache:
    key: uv-$CI_COMMIT_REF_SLUG
    paths:
      - .cache/uv
  before_script:
    - pip install "uv>=0.5,<1" && uv sync --frozen

# ── Validate ──────────────────────────────────────────────────
lint:
  stage: validate
  timeout: 10 minutes
  script:
    - uv run ruff check .
    - uv run mypy .
    - uv run pytest -v
    - uv run haute lint
    - uv run haute deploy --dry-run
  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH

# ── Staging ───────────────────────────────────────────────────
deploy-staging:
  stage: deploy-staging
  timeout: 15 minutes
  resource_group: deploy
  variables:
{secrets_env}
  script:
    - uv run haute deploy --endpoint-suffix "-staging"
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH

# ── Smoke test ────────────────────────────────────────────────
smoke-test:
  stage: smoke-test
  timeout: 10 minutes
  resource_group: deploy
  variables:
{secrets_env}
  script:
    - uv run haute smoke --endpoint-suffix "-staging"
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH

# ── Impact analysis ──────────────────────────────────────────
impact-analysis:
  stage: impact-analysis
  timeout: 10 minutes
  resource_group: deploy
  variables:
{secrets_env}
  script:
    - uv run haute impact --endpoint-suffix "-staging"
  artifacts:
    paths:
      - impact_report.md
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH

# ── Production (manual approval) ─────────────────────────────
deploy-production:
  stage: deploy-production
  timeout: 15 minutes
  resource_group: deploy
  variables:
{secrets_env}
  script:
    - uv run haute deploy
    - |
      VERSION=$(uv run haute status --version-only 2>/dev/null || echo "unknown")
      git tag "deploy/v$VERSION"
      git push origin "deploy/v$VERSION"
  when: manual
  allow_failure: false
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
"""


def _gitlab_secrets_env(target: str) -> str:
    """Return the variables: block for GitLab CI, indented for YAML."""
    indent = "    "
    if target == "databricks":
        return (
            f"{indent}DATABRICKS_HOST: $DATABRICKS_HOST\n"
            f"{indent}DATABRICKS_TOKEN: $DATABRICKS_TOKEN"
        )
    if target == "sagemaker":
        return (
            f"{indent}AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID\n"
            f"{indent}AWS_SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY\n"
            f"{indent}AWS_DEFAULT_REGION: $AWS_DEFAULT_REGION\n"
            f"{indent}SAGEMAKER_ROLE_ARN: $SAGEMAKER_ROLE_ARN"
        )
    if target == "azure-ml":
        return (
            f"{indent}AZURE_SUBSCRIPTION_ID: $AZURE_SUBSCRIPTION_ID\n"
            f"{indent}AZURE_TENANT_ID: $AZURE_TENANT_ID\n"
            f"{indent}AZURE_CLIENT_ID: $AZURE_CLIENT_ID\n"
            f"{indent}AZURE_CLIENT_SECRET: $AZURE_CLIENT_SECRET"
        )
    if target == "docker":
        return (
            f"{indent}DOCKER_USERNAME: $DOCKER_USERNAME\n"
            f"{indent}DOCKER_PASSWORD: $DOCKER_PASSWORD"
        )
    msg = f"Unknown target: {target}"
    raise ValueError(msg)


# ── Azure DevOps ─────────────────────────────────────────────────────


def azure_devops_yml(target: str) -> str:
    """Combined CI + deploy pipeline for Azure DevOps.

    Uses stages to enforce ordering.  The ``DeployProduction`` stage
    uses an Environment with approval checks so a reviewer must approve
    before production deployment proceeds.

    Credentials are read from an Azure DevOps variable group named
    ``haute-credentials`` — the env var names are identical to other
    CI providers.
    """
    secrets_env = _azure_devops_secrets_env(target)

    return f"""\
trigger:
  branches:
    include: [main]
  paths:
    include:
      - "*.py"
      - haute.toml
      - data/
      - models/
      - tests/quotes/

pr:
  branches:
    include: [main]

variables:
  CI: "true"

stages:
  # ── Validate (runs on PR and push to main) ───────────────────
  - stage: Validate
    jobs:
      - job: lint
        displayName: Lint & Format
        timeoutInMinutes: 10
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.11"
          - script: pip install "uv>=0.5,<1" && uv sync --frozen
            displayName: Install dependencies
          - script: uv run ruff check .
            displayName: Ruff check
          - script: uv run ruff format --check .
            displayName: Ruff format check

      - job: typecheck
        displayName: Type Check
        timeoutInMinutes: 10
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.11"
          - script: pip install "uv>=0.5,<1" && uv sync --frozen
            displayName: Install dependencies
          - script: uv run mypy .
            displayName: Mypy

      - job: test
        displayName: Test
        timeoutInMinutes: 10
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.11"
          - script: pip install "uv>=0.5,<1" && uv sync --frozen
            displayName: Install dependencies
          - script: uv run pytest -v
            displayName: Pytest

      - job: pipeline_validate
        displayName: Pipeline Validation
        timeoutInMinutes: 10
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.11"
          - script: pip install "uv>=0.5,<1" && uv sync --frozen
            displayName: Install dependencies
          - script: uv run haute lint
            displayName: Lint pipeline
          - script: uv run haute deploy --dry-run
            displayName: Dry-run deploy

  # ── Deploy to staging (only on push to main) ─────────────────
  - stage: DeployStaging
    displayName: Deploy → Staging
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    variables:
      - group: haute-credentials
    jobs:
      - job: deploy_staging
        displayName: Deploy to staging
        timeoutInMinutes: 15
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.11"
          - script: pip install "uv>=0.5,<1" && uv sync --frozen
            displayName: Install dependencies
          - script: uv run haute deploy --endpoint-suffix "-staging"
            displayName: Deploy staging
            env:
{secrets_env}

  # ── Smoke test staging ───────────────────────────────────────
  - stage: SmokeTest
    displayName: Smoke Test Staging
    dependsOn: DeployStaging
    variables:
      - group: haute-credentials
    jobs:
      - job: smoke_test
        displayName: Score test quotes against staging
        timeoutInMinutes: 10
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.11"
          - script: pip install "uv>=0.5,<1" && uv sync --frozen
            displayName: Install dependencies
          - script: uv run haute smoke --endpoint-suffix "-staging"
            displayName: Smoke test
            env:
{secrets_env}

  # ── Impact analysis ──────────────────────────────────────────
  - stage: ImpactAnalysis
    displayName: Impact Analysis
    dependsOn: SmokeTest
    variables:
      - group: haute-credentials
    jobs:
      - job: impact
        displayName: Compare staging vs production
        timeoutInMinutes: 10
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
          - task: UsePythonVersion@0
            inputs:
              versionSpec: "3.11"
          - script: pip install "uv>=0.5,<1" && uv sync --frozen
            displayName: Install dependencies
          - script: uv run haute impact --endpoint-suffix "-staging"
            displayName: Impact analysis
            env:
{secrets_env}
          - publish: impact_report.md
            artifact: impact-report
            condition: succeededOrFailed()

  # ── Deploy to production (manual approval) ───────────────────
  - stage: DeployProduction
    displayName: Deploy → Production
    dependsOn: ImpactAnalysis
    variables:
      - group: haute-credentials
    jobs:
      - deployment: deploy_production
        displayName: Deploy to production
        timeoutInMinutes: 15
        pool:
          vmImage: ubuntu-latest
        environment: production
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                - task: UsePythonVersion@0
                  inputs:
                    versionSpec: "3.11"
                - script: pip install "uv>=0.5,<1" && uv sync --frozen
                  displayName: Install dependencies
                - script: uv run haute deploy
                  displayName: Deploy production
                  env:
{secrets_env}
                - script: |
                    set -euo pipefail
                    VERSION=$(uv run haute status --version-only 2>/dev/null || echo "unknown")
                    git tag "deploy/v$VERSION"
                    git push origin "deploy/v$VERSION"
                  displayName: Tag release
"""


def _azure_devops_secrets_env(target: str) -> str:
    """Return the env: block for Azure DevOps pipeline secrets, indented for YAML."""
    indent = "              "
    if target == "databricks":
        return (
            f"{indent}DATABRICKS_HOST: $(DATABRICKS_HOST)\n"
            f"{indent}DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)"
        )
    if target == "sagemaker":
        return (
            f"{indent}AWS_ACCESS_KEY_ID: $(AWS_ACCESS_KEY_ID)\n"
            f"{indent}AWS_SECRET_ACCESS_KEY: $(AWS_SECRET_ACCESS_KEY)\n"
            f"{indent}AWS_DEFAULT_REGION: $(AWS_DEFAULT_REGION)\n"
            f"{indent}SAGEMAKER_ROLE_ARN: $(SAGEMAKER_ROLE_ARN)"
        )
    if target == "azure-ml":
        return (
            f"{indent}AZURE_SUBSCRIPTION_ID: $(AZURE_SUBSCRIPTION_ID)\n"
            f"{indent}AZURE_TENANT_ID: $(AZURE_TENANT_ID)\n"
            f"{indent}AZURE_CLIENT_ID: $(AZURE_CLIENT_ID)\n"
            f"{indent}AZURE_CLIENT_SECRET: $(AZURE_CLIENT_SECRET)"
        )
    if target == "docker":
        return (
            f"{indent}DOCKER_USERNAME: $(DOCKER_USERNAME)\n"
            f"{indent}DOCKER_PASSWORD: $(DOCKER_PASSWORD)"
        )
    msg = f"Unknown target: {target}"
    raise ValueError(msg)


# ── Pre-commit hook ───────────────────────────────────────────────────


def pre_commit_hook() -> str:
    """Generate a Git pre-commit hook that auto-formats with ruff."""
    return """\
#!/bin/sh
# Haute pre-commit hook - auto-format staged Python files with ruff.
# Installed by `haute init`. To reinstall: cp .githooks/pre-commit .git/hooks/

FILES=$(git diff --cached --name-only --diff-filter=ACM -- '*.py')
[ -z "$FILES" ] && exit 0

uv run ruff format $FILES
uv run ruff check --fix $FILES 2>/dev/null || true
git add $FILES
"""


# ── Starter files ─────────────────────────────────────────────────────


def starter_pipeline(name: str) -> str:
    """Generate the starter ``main.py`` pipeline."""
    return f'''\
"""Pipeline: {name}"""

import polars as pl
import haute

pipeline = haute.Pipeline("{name}", description="")
'''


def starter_test(name: str) -> str:
    """Generate a starter ``tests/test_pipeline.py`` so pytest passes out of the box."""
    return f'''\
"""Starter tests for {name}."""

from pathlib import Path


def test_pipeline_parses():
    """Pipeline file is valid Python and contains a haute Pipeline."""
    pipeline_path = Path(__file__).resolve().parent.parent / "main.py"
    source = pipeline_path.read_text()
    compile(source, str(pipeline_path), "exec")
    assert "haute.Pipeline" in source
'''


def starter_test_quote() -> str:
    """Generate the starter ``tests/quotes/example.json``."""
    return """\
[
  {
    "_description": "Example quote - replace with your own fields",
    "id": 1,
    "field_a": "value",
    "field_b": 42
  }
]
"""
