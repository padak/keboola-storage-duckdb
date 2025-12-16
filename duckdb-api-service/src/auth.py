"""Authentication service for DuckDB Storage API.

This module provides stateless utility functions for API key generation,
hashing, and verification. The actual key storage and validation against
the database is handled by database.py and dependencies.py.

Key Format: proj_{project_id}_admin_{random_hex_16}
Example: proj_123_admin_a1b2c3d4e5f6g7h8
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

    Returns the part of the key before the random component (approximately
    the first 20 characters), which is safe to log without exposing the
    secret random portion.

    Args:
        key: The full API key

    Returns:
        The key prefix (e.g., "proj_123_admin_...")

    Example:
        >>> key = "proj_123_admin_a1b2c3d4e5f6g7h8"
        >>> get_key_prefix(key)
        'proj_123_admin_...'
    """
    # Split on underscores and take first 3 parts: proj, {id}, admin
    parts = key.split("_")
    if len(parts) >= 4:  # Standard format: proj_{id}_admin_{random}
        prefix = "_".join(parts[:3])  # "proj_{id}_admin"
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
