"""Tests for haute._scaffold - template generation for ``haute init``."""

from haute._scaffold import (
    env_example,
    github_ci_yml,
    github_deploy_yml,
    haute_toml,
    starter_pipeline,
    starter_test_quote,
)


class TestHauteToml:
    """haute_toml() generates target-specific config with no leaking sections."""

    def test_databricks_only_contains_databricks_section(self) -> None:
        result = haute_toml("motor", "databricks", "github")
        assert "[deploy.databricks]" in result
        assert "[deploy.docker]" not in result
        assert "[deploy.sagemaker]" not in result
        assert "[deploy.azure-ml]" not in result

    def test_docker_only_contains_docker_section(self) -> None:
        result = haute_toml("motor", "docker", "github")
        assert "[deploy.docker]" in result
        assert "[deploy.databricks]" not in result
        assert "[deploy.sagemaker]" not in result
        assert "[deploy.azure-ml]" not in result

    def test_sagemaker_only_contains_sagemaker_section(self) -> None:
        result = haute_toml("motor", "sagemaker", "gitlab")
        assert "[deploy.sagemaker]" in result
        assert "[deploy.databricks]" not in result

    def test_azure_ml_only_contains_azure_section(self) -> None:
        result = haute_toml("motor", "azure-ml", "github")
        assert "[deploy.azure-ml]" in result
        assert "[deploy.databricks]" not in result

    def test_project_name_substituted(self) -> None:
        result = haute_toml("my_pipeline", "databricks", "github")
        assert 'name = "my_pipeline"' in result
        assert 'model_name = "my_pipeline"' in result
        assert 'endpoint_name = "my_pipeline"' in result

    def test_ci_provider_set(self) -> None:
        result = haute_toml("motor", "databricks", "none")
        assert 'provider = "none"' in result

    def test_team_defaults(self) -> None:
        result = haute_toml("motor", "databricks", "github")
        assert "min_approvers = 2" in result

    def test_safety_section_present(self) -> None:
        result = haute_toml("motor", "databricks", "github")
        assert "[safety]" in result
        assert "impact_dataset" in result

    def test_staging_section(self) -> None:
        result = haute_toml("motor", "databricks", "github")
        assert "[ci.staging]" in result
        assert 'endpoint_suffix = "-staging"' in result

    def test_unknown_target_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Unknown target"):
            haute_toml("motor", "unknown", "github")


class TestEnvExample:
    """env_example() generates only the chosen target's credentials."""

    def test_databricks_creds_only(self) -> None:
        result = env_example("databricks")
        assert "DATABRICKS_HOST" in result
        assert "DATABRICKS_TOKEN" in result
        assert "AWS_ACCESS_KEY" not in result
        assert "AZURE_" not in result

    def test_sagemaker_creds_only(self) -> None:
        result = env_example("sagemaker")
        assert "AWS_ACCESS_KEY_ID" in result
        assert "SAGEMAKER_ROLE_ARN" in result
        assert "DATABRICKS_" not in result

    def test_azure_creds_only(self) -> None:
        result = env_example("azure-ml")
        assert "AZURE_SUBSCRIPTION_ID" in result
        assert "DATABRICKS_" not in result
        assert "AWS_" not in result

    def test_docker_creds_only(self) -> None:
        result = env_example("docker")
        assert "DOCKER_USERNAME" in result
        assert "DATABRICKS_" not in result

    def test_unknown_target_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Unknown target"):
            env_example("unknown")


class TestGithubCiYml:
    """github_ci_yml() generates valid CI workflow."""

    def test_contains_required_jobs(self) -> None:
        result = github_ci_yml()
        assert "lint:" in result
        assert "typecheck:" in result
        assert "test:" in result
        assert "pipeline-validate:" in result

    def test_triggers_on_pr_to_main(self) -> None:
        result = github_ci_yml()
        assert "pull_request:" in result
        assert "branches: [main]" in result

    def test_uses_uv(self) -> None:
        result = github_ci_yml()
        assert "astral-sh/setup-uv@v4" in result
        assert "uv sync --frozen" in result


class TestGithubDeployYml:
    """github_deploy_yml() generates valid deploy workflow with correct secrets."""

    def test_databricks_secrets(self) -> None:
        result = github_deploy_yml("databricks")
        assert "secrets.DATABRICKS_HOST" in result
        assert "secrets.DATABRICKS_TOKEN" in result
        assert "secrets.AWS_ACCESS_KEY_ID" not in result

    def test_sagemaker_secrets(self) -> None:
        result = github_deploy_yml("sagemaker")
        assert "secrets.AWS_ACCESS_KEY_ID" in result
        assert "secrets.SAGEMAKER_ROLE_ARN" in result
        assert "secrets.DATABRICKS_HOST" not in result

    def test_contains_staging_and_impact(self) -> None:
        result = github_deploy_yml("databricks")
        assert "deploy-staging:" in result
        assert "smoke-test:" in result
        assert "impact-analysis:" in result
        assert "deploy-production:" not in result

    def test_deploy_prod_yml_contains_production(self) -> None:
        from haute._scaffold import github_deploy_prod_yml

        result = github_deploy_prod_yml("databricks")
        assert "deploy-production:" in result
        assert "workflow_dispatch" in result
        assert "secrets.DATABRICKS_HOST" in result

    def test_deploy_prod_yml_tags_release(self) -> None:
        from haute._scaffold import github_deploy_prod_yml

        result = github_deploy_prod_yml("databricks")
        assert "Tag release" in result

    def test_triggers_on_push_to_main(self) -> None:
        result = github_deploy_yml("databricks")
        assert "push:" in result
        assert "branches: [main]" in result


class TestStarterFiles:
    """starter_pipeline() and starter_test_quote() generate valid content."""

    def test_pipeline_has_name(self) -> None:
        result = starter_pipeline("my_project")
        assert 'Pipeline("my_project"' in result
        assert "import haute" in result
        assert "import polars" in result

    def test_test_quote_is_valid_json_array(self) -> None:
        import json

        result = starter_test_quote()
        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert len(parsed) == 1
