"""
Snapshot configuration with hierarchical inheritance.

ADR-004: Configuration is resolved from System -> Project -> Bucket -> Table
Each level can override values from the level above.
"""

from copy import deepcopy
from typing import Any

import structlog

from src.database import metadata_db

logger = structlog.get_logger()


# System defaults (hardcoded) - the base configuration
SYSTEM_DEFAULTS: dict[str, Any] = {
    "auto_snapshot_triggers": {
        "drop_table": True,        # Snapshot before DROP TABLE
        "truncate_table": False,   # Snapshot before TRUNCATE
        "delete_all_rows": False,  # Snapshot before DELETE FROM without WHERE
        "drop_column": False,      # Snapshot before ALTER TABLE DROP COLUMN
    },
    "retention": {
        "manual_days": 90,         # Manual snapshots: 90 days
        "auto_days": 7,            # Auto snapshots: 7 days
    },
    "enabled": True,               # Master switch
}


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """
    Deep merge override into base, returning new dict.

    Only non-None values in override replace base values.
    Nested dicts are merged recursively.
    """
    result = deepcopy(base)

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        elif value is not None:
            result[key] = value

    return result


def flatten_keys(d: dict[str, Any], prefix: str = "") -> list[str]:
    """
    Flatten nested dict keys.

    Example: {'a': {'b': 1}} -> ['a.b']
    """
    keys = []
    for k, v in d.items():
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            keys.extend(flatten_keys(v, full_key))
        else:
            keys.append(full_key)
    return keys


def get_entity_id(
    project_id: str,
    bucket_name: str | None = None,
    table_name: str | None = None,
) -> tuple[str, str]:
    """
    Generate entity type and ID for snapshot settings.

    Returns:
        Tuple of (entity_type, entity_id)
    """
    if table_name and bucket_name:
        return ("table", f"{project_id}/{bucket_name}/{table_name}")
    elif bucket_name:
        return ("bucket", f"{project_id}/{bucket_name}")
    else:
        return ("project", project_id)


def resolve_snapshot_config(
    project_id: str,
    bucket_name: str | None = None,
    table_name: str | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    """
    Resolve effective snapshot config with inheritance.

    The inheritance chain is:
    System Defaults -> Project Settings -> Bucket Settings -> Table Settings

    Args:
        project_id: Project ID
        bucket_name: Optional bucket name
        table_name: Optional table name (requires bucket_name)

    Returns:
        Tuple of (effective_config, inheritance_sources)
        - effective_config: The merged configuration
        - inheritance_sources: Dict mapping each config key to its source
          ('system', 'project', 'bucket', or 'table')
    """
    config = deepcopy(SYSTEM_DEFAULTS)
    sources: dict[str, str] = {key: "system" for key in flatten_keys(config)}

    # Layer 1: Project settings
    project_settings = metadata_db.get_snapshot_settings("project", project_id)
    if project_settings and project_settings.get("config"):
        project_config = project_settings["config"]
        config = deep_merge(config, project_config)
        for key in flatten_keys(project_config):
            sources[key] = "project"

    # Layer 2: Bucket settings (if bucket specified)
    if bucket_name:
        bucket_id = f"{project_id}/{bucket_name}"
        bucket_settings = metadata_db.get_snapshot_settings("bucket", bucket_id)
        if bucket_settings and bucket_settings.get("config"):
            bucket_config = bucket_settings["config"]
            config = deep_merge(config, bucket_config)
            for key in flatten_keys(bucket_config):
                sources[key] = "bucket"

    # Layer 3: Table settings (if table specified)
    if table_name and bucket_name:
        table_id = f"{project_id}/{bucket_name}/{table_name}"
        table_settings = metadata_db.get_snapshot_settings("table", table_id)
        if table_settings and table_settings.get("config"):
            table_config = table_settings["config"]
            config = deep_merge(config, table_config)
            for key in flatten_keys(table_config):
                sources[key] = "table"

    return config, sources


def get_local_config(
    project_id: str,
    bucket_name: str | None = None,
    table_name: str | None = None,
) -> dict[str, Any] | None:
    """
    Get only the local (non-inherited) configuration for an entity.

    Returns:
        The local config dict, or None if no local settings exist
    """
    entity_type, entity_id = get_entity_id(project_id, bucket_name, table_name)
    settings = metadata_db.get_snapshot_settings(entity_type, entity_id)

    if settings and settings.get("config"):
        return settings["config"]

    return None


def should_create_snapshot(
    project_id: str,
    bucket_name: str,
    table_name: str,
    trigger: str,
) -> bool:
    """
    Check if a snapshot should be created for a given trigger.

    Args:
        project_id: Project ID
        bucket_name: Bucket name
        table_name: Table name
        trigger: One of 'drop_table', 'truncate_table', 'delete_all_rows', 'drop_column'

    Returns:
        True if snapshot should be created
    """
    config, _ = resolve_snapshot_config(project_id, bucket_name, table_name)

    # Check master switch
    if not config.get("enabled", True):
        logger.debug(
            "snapshot_disabled",
            project_id=project_id,
            bucket_name=bucket_name,
            table_name=table_name,
        )
        return False

    # Check specific trigger
    triggers = config.get("auto_snapshot_triggers", {})
    should_snapshot = triggers.get(trigger, False)

    logger.debug(
        "snapshot_trigger_check",
        project_id=project_id,
        bucket_name=bucket_name,
        table_name=table_name,
        trigger=trigger,
        result=should_snapshot,
    )

    return should_snapshot


def get_retention_days(
    project_id: str,
    bucket_name: str | None = None,
    table_name: str | None = None,
    snapshot_type: str = "manual",
) -> int:
    """
    Get retention period in days for a snapshot.

    Args:
        project_id: Project ID
        bucket_name: Optional bucket name
        table_name: Optional table name
        snapshot_type: 'manual' or any auto type ('auto_predrop', etc.)

    Returns:
        Number of days until expiration
    """
    config, _ = resolve_snapshot_config(project_id, bucket_name, table_name)
    retention = config.get("retention", {})

    if snapshot_type == "manual":
        return retention.get("manual_days", 90)
    else:
        return retention.get("auto_days", 7)


def validate_config(config: dict[str, Any]) -> list[str]:
    """
    Validate a snapshot configuration.

    Args:
        config: Configuration dict to validate

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # Validate auto_snapshot_triggers
    if "auto_snapshot_triggers" in config:
        triggers = config["auto_snapshot_triggers"]
        if not isinstance(triggers, dict):
            errors.append("auto_snapshot_triggers must be an object")
        else:
            valid_triggers = {"drop_table", "truncate_table", "delete_all_rows", "drop_column"}
            for key, value in triggers.items():
                if key not in valid_triggers:
                    errors.append(f"Unknown trigger: {key}. Valid: {valid_triggers}")
                if not isinstance(value, bool):
                    errors.append(f"Trigger {key} must be a boolean")

    # Validate retention
    if "retention" in config:
        retention = config["retention"]
        if not isinstance(retention, dict):
            errors.append("retention must be an object")
        else:
            valid_retention = {"manual_days", "auto_days"}
            for key, value in retention.items():
                if key not in valid_retention:
                    errors.append(f"Unknown retention key: {key}. Valid: {valid_retention}")
                if not isinstance(value, int) or value < 1:
                    errors.append(f"Retention {key} must be a positive integer")
                if isinstance(value, int) and value > 3650:  # 10 years max
                    errors.append(f"Retention {key} cannot exceed 3650 days")

    # Validate enabled
    if "enabled" in config:
        if not isinstance(config["enabled"], bool):
            errors.append("enabled must be a boolean")

    return errors
