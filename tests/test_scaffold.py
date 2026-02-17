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

    def test_container_only_contains_container_section(self) -> None:
        result = haute_toml("motor", "container", "github")
        assert "[deploy.container]" in result
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

    def test_azure_container_apps_contains_both_sections(self) -> None:
        result = haute_toml("motor", "azure-container-apps", "github")
        assert "[deploy.container]" in result
        assert "[deploy.azure-container-apps]" in result
        assert "[deploy.databricks]" not in result

    def test_aws_ecs_contains_both_sections(self) -> None:
        result = haute_toml("motor", "aws-ecs", "github")
        assert "[deploy.container]" in result
        assert "[deploy.aws-ecs]" in result
        assert "[deploy.databricks]" not in result

    def test_gcp_run_contains_both_sections(self) -> None:
        result = haute_toml("motor", "gcp-run", "github")
        assert "[deploy.container]" in result
        assert "[deploy.gcp-run]" in result
        assert "[deploy.databricks]" not in result

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

    def test_container_creds_only(self) -> None:
        result = env_example("container")
        assert "DOCKER_USERNAME" in result
        assert "DATABRICKS_" not in result

    def test_azure_container_apps_creds(self) -> None:
        result = env_example("azure-container-apps")
        assert "DOCKER_USERNAME" in result
        assert "AZURE_SUBSCRIPTION_ID" in result
        assert "DATABRICKS_" not in result

    def test_aws_ecs_creds(self) -> None:
        result = env_example("aws-ecs")
        assert "DOCKER_USERNAME" in result
        assert "AWS_ACCESS_KEY_ID" in result
        assert "DATABRICKS_" not in result

    def test_gcp_run_creds(self) -> None:
        result = env_example("gcp-run")
        assert "DOCKER_USERNAME" in result
        assert "GCP_PROJECT_ID" in result
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


class TestGithubDeployProdYml:
    """github_deploy_prod_yml() includes SHA pinning and strict tag handling."""

    def test_sha_input_present(self) -> None:
        from haute._scaffold import github_deploy_prod_yml

        result = github_deploy_prod_yml("databricks")
        assert "inputs:" in result
        assert "sha:" in result

    def test_sha_verification_step(self) -> None:
        from haute._scaffold import github_deploy_prod_yml

        result = github_deploy_prod_yml("databricks")
        assert "Verify commit matches staged SHA" in result
        assert "GITHUB_SHA" in result

    def test_tag_step_uses_strict_mode(self) -> None:
        from haute._scaffold import github_deploy_prod_yml

        result = github_deploy_prod_yml("databricks")
        assert "set -euo pipefail" in result
        assert "|| true" not in result

    def test_timeout_set(self) -> None:
        from haute._scaffold import github_deploy_prod_yml

        result = github_deploy_prod_yml("databricks")
        assert "timeout-minutes:" in result

    def test_python_version_pinned(self) -> None:
        from haute._scaffold import github_deploy_prod_yml

        result = github_deploy_prod_yml("databricks")
        assert 'python-version: "3.11"' in result


class TestGithubDeployYmlImprovements:
    """Verify engineering improvements in deploy-staging workflow."""

    def test_impact_report_uploaded_as_artifact(self) -> None:
        result = github_deploy_yml("databricks")
        assert "upload-artifact@v4" in result
        assert "impact-report" in result

    def test_deployed_sha_recorded(self) -> None:
        result = github_deploy_yml("databricks")
        assert "GITHUB_SHA" in result
        assert "GITHUB_STEP_SUMMARY" in result

    def test_timeouts_on_all_jobs(self) -> None:
        result = github_deploy_yml("databricks")
        assert result.count("timeout-minutes:") >= 4

    def test_python_version_pinned(self) -> None:
        result = github_deploy_yml("databricks")
        assert 'python-version: "3.11"' in result


class TestGithubCiYmlImprovements:
    """Verify engineering improvements in CI workflow."""

    def test_timeouts_on_all_jobs(self) -> None:
        result = github_ci_yml()
        assert result.count("timeout-minutes:") >= 4

    def test_python_version_pinned(self) -> None:
        result = github_ci_yml()
        assert 'python-version: "3.11"' in result


class TestGitlabCiYml:
    """gitlab_ci_yml() generates valid GitLab CI pipeline."""

    def test_contains_all_stages(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("databricks")
        assert "validate" in result
        assert "deploy-staging" in result
        assert "smoke-test" in result
        assert "impact-analysis" in result
        assert "deploy-production" in result

    def test_secrets_not_at_top_level(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("databricks")
        lines = result.split("\n")
        # Top-level variables: block should not contain secrets
        in_top_variables = False
        for line in lines:
            if line == "variables:" and not line.startswith(" "):
                in_top_variables = True
                continue
            if in_top_variables and not line.startswith(" ") and line.strip():
                in_top_variables = False
            if in_top_variables:
                assert "DATABRICKS_HOST" not in line

    def test_secrets_in_deploy_jobs_only(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("databricks")
        # Secrets should appear in the deploy-staging, smoke-test, impact, production sections
        assert "DATABRICKS_HOST" in result
        # The lint section should NOT have variables with secrets
        lint_section = result.split("# ── Validate")[1].split("# ── Staging")[0]
        assert "DATABRICKS_HOST" not in lint_section

    def test_concurrency_control(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("databricks")
        assert "resource_group: deploy" in result

    def test_timeouts(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("databricks")
        assert "timeout:" in result

    def test_caching(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("databricks")
        assert "cache:" in result
        assert ".cache/uv" in result

    def test_uv_version_pinned(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("databricks")
        assert '"uv>=0.5,<1"' in result

    def test_databricks_secrets(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("databricks")
        assert "$DATABRICKS_HOST" in result
        assert "$DATABRICKS_TOKEN" in result

    def test_container_secrets(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("container")
        assert "$DOCKER_USERNAME" in result
        assert "$DOCKER_PASSWORD" in result

    def test_azure_container_apps_secrets(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("azure-container-apps")
        assert "$DOCKER_USERNAME" in result
        assert "$AZURE_SUBSCRIPTION_ID" in result

    def test_aws_ecs_secrets(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("aws-ecs")
        assert "$DOCKER_USERNAME" in result
        assert "$AWS_ACCESS_KEY_ID" in result

    def test_gcp_run_secrets(self) -> None:
        from haute._scaffold import gitlab_ci_yml

        result = gitlab_ci_yml("gcp-run")
        assert "$DOCKER_USERNAME" in result
        assert "$GCP_PROJECT_ID" in result


class TestAzureDevopsYml:
    """azure_devops_yml() generates valid Azure DevOps pipeline."""

    def test_contains_all_stages(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        assert "Validate" in result
        assert "DeployStaging" in result
        assert "SmokeTest" in result
        assert "ImpactAnalysis" in result
        assert "DeployProduction" in result

    def test_ci_env_var_set(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        assert 'CI: "true"' in result

    def test_secrets_not_in_validate_stage(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        validate_section = result.split("# ── Deploy to staging")[0]
        assert "haute-credentials" not in validate_section

    def test_secrets_in_deploy_stages(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        deploy_onwards = result.split("# ── Deploy to staging")[1]
        assert "haute-credentials" in deploy_onwards

    def test_python_version_pinned(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        assert "UsePythonVersion@0" in result
        assert 'versionSpec: "3.11"' in result

    def test_uv_version_pinned(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        assert '"uv>=0.5,<1"' in result

    def test_timeouts(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        assert "timeoutInMinutes:" in result

    def test_tag_step_uses_strict_mode(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        assert "set -euo pipefail" in result
        assert "|| true" not in result

    def test_path_filters_use_prefix(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        assert "- data/" in result
        assert "- models/" in result

    def test_databricks_secrets(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("databricks")
        assert "$(DATABRICKS_HOST)" in result
        assert "$(DATABRICKS_TOKEN)" in result

    def test_sagemaker_secrets(self) -> None:
        from haute._scaffold import azure_devops_yml

        result = azure_devops_yml("sagemaker")
        assert "$(AWS_ACCESS_KEY_ID)" in result
        assert "$(SAGEMAKER_ROLE_ARN)" in result
        assert "$(DATABRICKS_HOST)" not in result


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
