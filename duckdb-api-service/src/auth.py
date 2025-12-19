"""Authentication service for DuckDB Storage API.

This module provides stateless utility functions for API key generation,
hashing, and verification. The actual key storage and validation against
the database is handled by database.py and dependencies.py.

Key Formats:
1. Project admin: proj_{project_id}_admin_{random_hex_16}
   Example: proj_123_admin_a1b2c3d4e5f6g7h8

2. Branch admin: proj_{project_id}_branch_{branch_id}_admin_{random_hex_16}
   Example: proj_123_branch_456_admin_a1b2c3d4e5f6g7h8

3. Branch read-only: proj_{project_id}_branch_{branch_id}_read_{random_hex_16}
   Example: proj_123_branch_456_read_a1b2c3d4e5f6g7h8
"""

import hashlib
import secrets
import structlog

logger = structlog.get_logger(__name__)


def generate_api_key(project_id: str) -> str:
    """
    Generate a new project admin API key.

    The key format is: proj_{project_id}_admin_{random_hex_16}
    This format allows:
    - Easy identification of which project the key belongs to
    - Clear indication of admin privileges
    - 16 bytes (128 bits) of cryptographic randomness

    Args:
        project_id: The numeric project ID (e.g., "123")

    Returns:
        A new API key in the format proj_{project_id}_admin_{random_hex}

    Example:
        >>> key = generate_api_key("123")
        >>> key.startswith("proj_123_admin_")
        True
        >>> len(key.split("_")[-1]) == 32  # 16 bytes = 32 hex chars
        True
    """
    # Generate 16 bytes (128 bits) of cryptographic random data
    random_hex = secrets.token_hex(16)

    # Construct the key with our standard format
    api_key = f"proj_{project_id}_admin_{random_hex}"

    logger.info(
        "generated_api_key",
        project_id=project_id,
        key_prefix=get_key_prefix(api_key),
    )

    return api_key


def generate_branch_key(project_id: str, branch_id: str, scope: str) -> str:
    """
    Generate a new branch-specific API key.

    The key format is: proj_{project_id}_branch_{branch_id}_{scope}_{random_hex_16}
    where scope is either 'admin' or 'read'.

    Args:
        project_id: The numeric project ID (e.g., "123")
        branch_id: The numeric branch ID (e.g., "456")
        scope: The key scope - either 'admin' or 'read'

    Returns:
        A new API key in the format proj_{project_id}_branch_{branch_id}_{scope}_{random_hex}

    Raises:
        ValueError: If scope is not 'admin' or 'read'

    Example:
        >>> key = generate_branch_key("123", "456", "admin")
        >>> key.startswith("proj_123_branch_456_admin_")
        True
        >>> key = generate_branch_key("123", "456", "read")
        >>> key.startswith("proj_123_branch_456_read_")
        True
    """
    if scope not in ("admin", "read"):
        raise ValueError(f"Invalid scope: {scope}. Must be 'admin' or 'read'")

    # Generate 16 bytes (128 bits) of cryptographic random data
    random_hex = secrets.token_hex(16)

    # Construct the key with branch format
    api_key = f"proj_{project_id}_branch_{branch_id}_{scope}_{random_hex}"

    logger.info(
        "generated_branch_key",
        project_id=project_id,
        branch_id=branch_id,
        scope=scope,
        key_prefix=get_key_prefix(api_key),
    )

    return api_key


def parse_key_info(key: str) -> dict:
    """
    Parse an API key and extract its components.

    Supports both legacy project keys and new branch-specific keys.

    Args:
        key: The API key to parse

    Returns:
        Dictionary with:
        - project_id: The project ID (str or None if invalid)
        - branch_id: The branch ID (str or None if not a branch key)
        - scope: The key scope ('admin', 'read', or None if invalid)
        - is_valid_format: Whether the key matches expected format (bool)

    Example:
        >>> parse_key_info("proj_123_admin_a1b2c3d4e5f6g7h8")
        {'project_id': '123', 'branch_id': None, 'scope': 'admin', 'is_valid_format': True}
        >>> parse_key_info("proj_123_branch_456_admin_a1b2c3d4e5f6g7h8")
        {'project_id': '123', 'branch_id': '456', 'scope': 'admin', 'is_valid_format': True}
        >>> parse_key_info("proj_123_branch_456_read_a1b2c3d4e5f6g7h8")
        {'project_id': '123', 'branch_id': '456', 'scope': 'read', 'is_valid_format': True}
        >>> parse_key_info("invalid_key")
        {'project_id': None, 'branch_id': None, 'scope': None, 'is_valid_format': False}
    """
    parts = key.split("_")

    # Check for branch key format: proj_{pid}_branch_{bid}_{scope}_{random}
    # Expected parts: ["proj", pid, "branch", bid, scope, random]
    if len(parts) == 6 and parts[0] == "proj" and parts[2] == "branch":
        project_id = parts[1]
        branch_id = parts[3]
        scope = parts[4]

        # Validate scope
        if scope in ("admin", "read"):
            return {
                "project_id": project_id,
                "branch_id": branch_id,
                "scope": scope,
                "is_valid_format": True,
            }

    # Check for legacy project admin key format: proj_{pid}_admin_{random}
    # Expected parts: ["proj", pid, "admin", random]
    if len(parts) == 4 and parts[0] == "proj" and parts[2] == "admin":
        project_id = parts[1]
        return {
            "project_id": project_id,
            "branch_id": None,
            "scope": "admin",
            "is_valid_format": True,
        }

    # Invalid format
    return {
        "project_id": None,
        "branch_id": None,
        "scope": None,
        "is_valid_format": False,
    }


def hash_key(key: str) -> str:
    """
    Hash an API key using SHA256.

    This is a one-way hash suitable for storage. Never store raw API keys.
    Keys should be hashed before storing in the database and compared using
    verify_key_hash().

    Args:
        key: The raw API key to hash

    Returns:
        Hexadecimal SHA256 hash of the key

    Example:
        >>> key = "proj_123_admin_a1b2c3d4e5f6g7h8"
        >>> key_hash = hash_key(key)
        >>> len(key_hash) == 64  # SHA256 = 256 bits = 64 hex chars
        True
    """
    return hashlib.sha256(key.encode()).hexdigest()


def get_key_prefix(key: str) -> str:
    """
    Extract a safe prefix from an API key for logging and display.

    Returns the part of the key before the random component, which is safe
    to log without exposing the secret random portion.

    Handles both legacy project keys and new branch-specific keys.

    Args:
        key: The full API key

    Returns:
        The key prefix with ellipsis (e.g., "proj_123_admin_..." or "proj_123_branch_456_admin_...")

    Example:
        >>> key = "proj_123_admin_a1b2c3d4e5f6g7h8"
        >>> get_key_prefix(key)
        'proj_123_admin_...'
        >>> key = "proj_123_branch_456_admin_a1b2c3d4e5f6g7h8"
        >>> get_key_prefix(key)
        'proj_123_branch_456_admin_...'
    """
    parts = key.split("_")

    # Branch key format: proj_{pid}_branch_{bid}_{scope}_{random}
    if len(parts) >= 6 and parts[2] == "branch":
        prefix = "_".join(parts[:5])  # "proj_{pid}_branch_{bid}_{scope}"
        return f"{prefix}_..."

    # Legacy project key format: proj_{pid}_admin_{random}
    if len(parts) >= 4:
        prefix = "_".join(parts[:3])  # "proj_{pid}_admin"
        return f"{prefix}_..."

    # For malformed or short keys, truncate and add ellipsis
    return key[:20] + "..." if len(key) > 20 else key + "..."


def verify_key_hash(key: str, key_hash: str) -> bool:
    """
    Verify that a raw API key matches its stored hash.

    This is a constant-time comparison to prevent timing attacks.

    Args:
        key: The raw API key to verify
        key_hash: The stored hash to compare against

    Returns:
        True if the key matches the hash, False otherwise

    Example:
        >>> key = "proj_123_admin_a1b2c3d4e5f6g7h8"
        >>> key_hash = hash_key(key)
        >>> verify_key_hash(key, key_hash)
        True
        >>> verify_key_hash("wrong_key", key_hash)
        False
    """
    computed_hash = hash_key(key)

    # Use secrets.compare_digest for constant-time comparison
    # This prevents timing attacks that could reveal information about the hash
    result = secrets.compare_digest(computed_hash, key_hash)

    logger.debug(
        "key_verification",
        key_prefix=get_key_prefix(key),
        verified=result,
    )

    return result
