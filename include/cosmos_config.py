"""Cosmos settings shared by the DuckDB and Postgres dbt DAGs."""

from pathlib import Path

from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig, TestBehavior

AIRFLOW_HOME = Path("/usr/local/airflow")
DBT_PROJECT = AIRFLOW_HOME / "include" / "dbt" / "shop"
DBT_EXECUTABLE = AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"  # isolated venv, see Dockerfile

project_config = ProjectConfig(DBT_PROJECT)
execution_config = ExecutionConfig(dbt_executable_path=str(DBT_EXECUTABLE))
# Tests run right after their model; tests touching two models (relationships)
# get their own task once *both* parents are built.
render_config = RenderConfig(
    test_behavior=TestBehavior.AFTER_EACH,
    should_detach_multiple_parents_tests=True,
)


def profile(target: str) -> ProfileConfig:
    # Same profiles.yml for every orchestrator; credentials come from env vars (.env).
    return ProfileConfig(
        profile_name="shop",
        target_name=target,
        profiles_yml_filepath=DBT_PROJECT / "profiles.yml",
    )
