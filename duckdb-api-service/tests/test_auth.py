"""Tests for the authentication service.

Tests the stateless auth utility functions including key generation,
hashing, prefix extraction, and verification.
"""

import pytest
from src.auth import (
    generate_api_key,
    generate_branch_key,
    hash_key,
    get_key_prefix,
    parse_key_info,
    verify_key_hash,
)


def test_generate_api_key_format():
    """Test that generated API keys follow the expected format."""
    key = generate_api_key("123")

    # Check prefix format
    assert key.startswith("proj_123_admin_"), "Key should start with proj_{id}_admin_"

    # Check structure: proj_123_admin_hexstring
    parts = key.split("_")
    assert len(parts) == 4, "Key should have 4 underscore-separated parts"
    assert parts[0] == "proj", "First part should be 'proj'"
    assert parts[1] == "123", "Second part should be project_id"
    assert parts[2] == "admin", "Third part should be 'admin'"

    # Check random part length (16 bytes = 32 hex chars)
    random_part = parts[3]
    assert len(random_part) == 32, "Random part should be 32 hex characters (16 bytes)"
    assert all(c in "0123456789abcdef" for c in random_part), "Random part should be hex"


def test_generate_api_key_uniqueness():
    """Test that each generated key is unique."""
    keys = [generate_api_key("123") for _ in range(10)]
    assert len(set(keys)) == 10, "All generated keys should be unique"


def test_generate_api_key_different_projects():
    """Test key generation for different project IDs."""
    key1 = generate_api_key("123")
    key2 = generate_api_key("456")
    key3 = generate_api_key("999")

    assert key1.startswith("proj_123_admin_")
    assert key2.startswith("proj_456_admin_")
    assert key3.startswith("proj_999_admin_")
    assert key1 != key2 != key3


def test_hash_key_deterministic():
    """Test that hashing the same key produces the same hash."""
    key = "proj_123_admin_a1b2c3d4e5f6g7h8"
    hash1 = hash_key(key)
    hash2 = hash_key(key)

    assert hash1 == hash2, "Same key should produce same hash"
    assert len(hash1) == 64, "SHA256 hash should be 64 hex characters"


def test_hash_key_different_for_different_keys():
    """Test that different keys produce different hashes."""
    key1 = generate_api_key("123")
    key2 = generate_api_key("123")

    hash1 = hash_key(key1)
    hash2 = hash_key(key2)

    assert hash1 != hash2, "Different keys should produce different hashes"


def test_get_key_prefix():
    """Test key prefix extraction for safe logging."""
    key = "proj_123_admin_a1b2c3d4e5f6g7h8"
    prefix = get_key_prefix(key)

    assert prefix == "proj_123_admin_...", "Prefix should mask the random part"
    assert "a1b2c3d4" not in prefix, "Random part should not be in prefix"


def test_get_key_prefix_short_key():
    """Test prefix extraction for short/malformed keys."""
    short_key = "short_key"
    prefix = get_key_prefix(short_key)

    # Should handle gracefully
    assert "..." in prefix
    assert len(prefix) <= len(short_key) + 3  # original + "..."


def test_verify_key_hash_correct_key():
    """Test that correct key verification succeeds."""
    key = generate_api_key("123")
    key_hash = hash_key(key)

    assert verify_key_hash(key, key_hash) is True


def test_verify_key_hash_wrong_key():
    """Test that wrong key verification fails."""
    key1 = generate_api_key("123")
    key2 = generate_api_key("123")
    key_hash = hash_key(key1)

    assert verify_key_hash(key2, key_hash) is False


def test_verify_key_hash_tampered_key():
    """Test that tampered key verification fails."""
    key = generate_api_key("123")
    key_hash = hash_key(key)

    # Tamper with the key
    tampered_key = key[:-1] + "x"

    assert verify_key_hash(tampered_key, key_hash) is False


def test_verify_key_hash_empty_key():
    """Test verification with empty key."""
    key = generate_api_key("123")
    key_hash = hash_key(key)

    assert verify_key_hash("", key_hash) is False


def test_end_to_end_workflow():
    """Test complete workflow: generate, hash, store, verify."""
    project_id = "789"

    # 1. Generate key
    api_key = generate_api_key(project_id)
    assert api_key.startswith(f"proj_{project_id}_admin_")

    # 2. Hash for storage (never store raw key)
    stored_hash = hash_key(api_key)
    assert len(stored_hash) == 64

    # 3. Get prefix for logging
    log_prefix = get_key_prefix(api_key)
    assert log_prefix == f"proj_{project_id}_admin_..."

    # 4. Verify correct key
    assert verify_key_hash(api_key, stored_hash) is True

    # 5. Verify wrong key fails
    wrong_key = generate_api_key(project_id)
    assert verify_key_hash(wrong_key, stored_hash) is False


def test_key_format_matches_documentation():
    """Test that key format matches CLAUDE.md specification."""
    # From CLAUDE.md: proj_{project_id}_admin_{random}
    key = generate_api_key("123")

    # Should match example: proj_123_admin_a1b2c3d4e5f6g7h8
    parts = key.split("_")
    assert parts[0] == "proj"
    assert parts[1] == "123"
    assert parts[2] == "admin"
    assert len(parts[3]) == 32  # Random hex part


# Branch key tests


def test_generate_branch_key_admin_format():
    """Test that generated branch admin keys follow the expected format."""
    key = generate_branch_key("123", "456", "admin")

    # Check prefix format
    assert key.startswith("proj_123_branch_456_admin_")

    # Check structure: proj_123_branch_456_admin_hexstring
    parts = key.split("_")
    assert len(parts) == 6, "Branch key should have 6 underscore-separated parts"
    assert parts[0] == "proj"
    assert parts[1] == "123"
    assert parts[2] == "branch"
    assert parts[3] == "456"
    assert parts[4] == "admin"

    # Check random part length (16 bytes = 32 hex chars)
    random_part = parts[5]
    assert len(random_part) == 32
    assert all(c in "0123456789abcdef" for c in random_part)


def test_generate_branch_key_read_format():
    """Test that generated branch read keys follow the expected format."""
    key = generate_branch_key("123", "456", "read")

    assert key.startswith("proj_123_branch_456_read_")

    parts = key.split("_")
    assert len(parts) == 6
    assert parts[4] == "read"


def test_generate_branch_key_invalid_scope():
    """Test that invalid scope raises ValueError."""
    with pytest.raises(ValueError, match="Invalid scope: write"):
        generate_branch_key("123", "456", "write")

    with pytest.raises(ValueError, match="Invalid scope: invalid"):
        generate_branch_key("123", "456", "invalid")


def test_generate_branch_key_uniqueness():
    """Test that each generated branch key is unique."""
    keys = [generate_branch_key("123", "456", "admin") for _ in range(10)]
    assert len(set(keys)) == 10


def test_parse_key_info_project_admin():
    """Test parsing legacy project admin keys."""
    key = "proj_123_admin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
    info = parse_key_info(key)

    assert info["project_id"] == "123"
    assert info["branch_id"] is None
    assert info["scope"] == "admin"
    assert info["is_valid_format"] is True


def test_parse_key_info_branch_admin():
    """Test parsing branch admin keys."""
    key = "proj_123_branch_456_admin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
    info = parse_key_info(key)

    assert info["project_id"] == "123"
    assert info["branch_id"] == "456"
    assert info["scope"] == "admin"
    assert info["is_valid_format"] is True


def test_parse_key_info_branch_read():
    """Test parsing branch read-only keys."""
    key = "proj_123_branch_456_read_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
    info = parse_key_info(key)

    assert info["project_id"] == "123"
    assert info["branch_id"] == "456"
    assert info["scope"] == "read"
    assert info["is_valid_format"] is True


def test_parse_key_info_invalid_format():
    """Test parsing invalid key formats."""
    invalid_keys = [
        "invalid_key",
        "proj_123",
        "proj_123_branch_456",
        "proj_123_branch_456_invalid_abc",  # Invalid scope
        "",
    ]

    for key in invalid_keys:
        info = parse_key_info(key)
        assert info["project_id"] is None
        assert info["branch_id"] is None
        assert info["scope"] is None
        assert info["is_valid_format"] is False


def test_parse_key_info_with_generated_keys():
    """Test parsing with actually generated keys."""
    project_key = generate_api_key("789")
    branch_admin_key = generate_branch_key("789", "101", "admin")
    branch_read_key = generate_branch_key("789", "101", "read")

    # Parse project key
    info = parse_key_info(project_key)
    assert info["project_id"] == "789"
    assert info["branch_id"] is None
    assert info["scope"] == "admin"
    assert info["is_valid_format"] is True

    # Parse branch admin key
    info = parse_key_info(branch_admin_key)
    assert info["project_id"] == "789"
    assert info["branch_id"] == "101"
    assert info["scope"] == "admin"
    assert info["is_valid_format"] is True

    # Parse branch read key
    info = parse_key_info(branch_read_key)
    assert info["project_id"] == "789"
    assert info["branch_id"] == "101"
    assert info["scope"] == "read"
    assert info["is_valid_format"] is True


def test_get_key_prefix_branch_keys():
    """Test key prefix extraction for branch keys."""
    admin_key = "proj_123_branch_456_admin_a1b2c3d4e5f6g7h8"
    read_key = "proj_123_branch_456_read_a1b2c3d4e5f6g7h8"

    admin_prefix = get_key_prefix(admin_key)
    read_prefix = get_key_prefix(read_key)

    assert admin_prefix == "proj_123_branch_456_admin_..."
    assert read_prefix == "proj_123_branch_456_read_..."
    assert "a1b2c3d4" not in admin_prefix
    assert "a1b2c3d4" not in read_prefix


def test_backwards_compatibility():
    """Test that old project keys still work with new functions."""
    # Generate old-style key
    project_key = generate_api_key("999")

    # Parse should work
    info = parse_key_info(project_key)
    assert info["is_valid_format"] is True
    assert info["project_id"] == "999"
    assert info["branch_id"] is None
    assert info["scope"] == "admin"

    # Prefix should work
    prefix = get_key_prefix(project_key)
    assert prefix == "proj_999_admin_..."

    # Hash and verify should work
    key_hash = hash_key(project_key)
    assert verify_key_hash(project_key, key_hash) is True


def test_branch_key_end_to_end():
    """Test complete branch key workflow: generate, hash, parse, verify."""
    project_id = "123"
    branch_id = "456"

    # 1. Generate admin key
    admin_key = generate_branch_key(project_id, branch_id, "admin")
    assert admin_key.startswith(f"proj_{project_id}_branch_{branch_id}_admin_")

    # 2. Hash for storage
    stored_hash = hash_key(admin_key)
    assert len(stored_hash) == 64

    # 3. Parse key info
    info = parse_key_info(admin_key)
    assert info["project_id"] == project_id
    assert info["branch_id"] == branch_id
    assert info["scope"] == "admin"
    assert info["is_valid_format"] is True

    # 4. Get prefix for logging
    log_prefix = get_key_prefix(admin_key)
    assert log_prefix == f"proj_{project_id}_branch_{branch_id}_admin_..."

    # 5. Verify correct key
    assert verify_key_hash(admin_key, stored_hash) is True

    # 6. Verify wrong key fails
    wrong_key = generate_branch_key(project_id, branch_id, "admin")
    assert verify_key_hash(wrong_key, stored_hash) is False
