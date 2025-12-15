#!/usr/bin/env python3
"""
End-to-end test script for DuckDB API Service.

This script tests the complete flow:
1. Backend initialization
2. Project CRUD
3. Bucket CRUD
4. Table CRUD with data
5. Preview functionality
6. Cleanup

Usage:
    # With server running on localhost:8000
    python scripts/e2e_test.py

    # With custom base URL
    python scripts/e2e_test.py --base-url http://localhost:8080

    # Verbose mode
    python scripts/e2e_test.py -v
"""

import argparse
import sys
import time
from datetime import datetime

import httpx


class Colors:
    """ANSI color codes for terminal output."""

    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


class E2ETestRunner:
    """End-to-end test runner for DuckDB API."""

    def __init__(self, base_url: str, verbose: bool = False):
        self.base_url = base_url.rstrip("/")
        self.verbose = verbose
        self.client = httpx.Client(base_url=self.base_url, timeout=30.0)
        self.passed = 0
        self.failed = 0
        self.test_project_id = f"e2e_test_{int(time.time())}"

    def log(self, message: str, color: str = ""):
        """Print log message."""
        if color:
            print(f"{color}{message}{Colors.END}")
        else:
            print(message)

    def log_verbose(self, message: str):
        """Print verbose log message."""
        if self.verbose:
            print(f"  {Colors.BLUE}{message}{Colors.END}")

    def test(self, name: str, func):
        """Run a test and track result."""
        print(f"\n{'='*60}")
        print(f"{Colors.BOLD}TEST: {name}{Colors.END}")
        print("=" * 60)

        try:
            func()
            self.passed += 1
            self.log(f"[PASS] {name}", Colors.GREEN)
            return True
        except AssertionError as e:
            self.failed += 1
            self.log(f"[FAIL] {name}: {e}", Colors.RED)
            return False
        except Exception as e:
            self.failed += 1
            self.log(f"[ERROR] {name}: {e}", Colors.RED)
            return False

    def run_all(self):
        """Run all E2E tests."""
        start_time = time.time()

        self.log(f"\n{Colors.BOLD}DuckDB API E2E Tests{Colors.END}")
        self.log(f"Base URL: {self.base_url}")
        self.log(f"Test Project ID: {self.test_project_id}")
        self.log(f"Started: {datetime.now().isoformat()}")

        # Run tests in order
        tests = [
            ("Health Check", self.test_health),
            ("Backend Init", self.test_backend_init),
            ("Create Project", self.test_create_project),
            ("Get Project", self.test_get_project),
            ("List Projects", self.test_list_projects),
            ("Project Stats (empty)", self.test_project_stats_empty),
            ("Create Bucket", self.test_create_bucket),
            ("List Buckets", self.test_list_buckets),
            ("Get Bucket", self.test_get_bucket),
            ("Create Table", self.test_create_table),
            ("Create Table with PK", self.test_create_table_with_pk),
            ("List Tables", self.test_list_tables),
            ("Get Table (ObjectInfo)", self.test_get_table),
            ("Preview Empty Table", self.test_preview_empty),
            ("Insert Test Data", self.test_insert_data),
            ("Preview with Data", self.test_preview_with_data),
            ("Preview with Limit", self.test_preview_with_limit),
            ("Project Stats (with data)", self.test_project_stats_with_data),
            ("Delete Table", self.test_delete_table),
            ("Delete Bucket", self.test_delete_bucket),
            ("Delete Project", self.test_delete_project),
        ]

        for name, func in tests:
            self.test(name, func)

        # Summary
        duration = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"{Colors.BOLD}SUMMARY{Colors.END}")
        print("=" * 60)
        print(f"Passed: {Colors.GREEN}{self.passed}{Colors.END}")
        print(f"Failed: {Colors.RED}{self.failed}{Colors.END}")
        print(f"Duration: {duration:.2f}s")

        return self.failed == 0

    # ==================== Test Methods ====================

    def test_health(self):
        """Test health endpoint."""
        resp = self.client.get("/health")
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["status"] == "healthy", f"Expected healthy, got {data['status']}"

    def test_backend_init(self):
        """Test backend initialization."""
        resp = self.client.post("/backend/init")
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["success"] is True, "Expected success=true"

    def test_create_project(self):
        """Test project creation."""
        resp = self.client.post(
            "/projects",
            json={"id": self.test_project_id, "name": "E2E Test Project"},
        )
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 201, f"Expected 201, got {resp.status_code}"
        data = resp.json()
        assert data["id"] == self.test_project_id
        assert data["name"] == "E2E Test Project"
        assert data["status"] == "active"

    def test_get_project(self):
        """Test getting project."""
        resp = self.client.get(f"/projects/{self.test_project_id}")
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["id"] == self.test_project_id

    def test_list_projects(self):
        """Test listing projects."""
        resp = self.client.get("/projects")
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert "projects" in data
        assert data["total"] >= 1

    def test_project_stats_empty(self):
        """Test project stats when empty."""
        resp = self.client.get(f"/projects/{self.test_project_id}/stats")
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["bucket_count"] == 0
        assert data["table_count"] == 0

    def test_create_bucket(self):
        """Test bucket creation."""
        resp = self.client.post(
            f"/projects/{self.test_project_id}/buckets",
            json={"name": "in_c_sales", "description": "Sales data bucket"},
        )
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 201, f"Expected 201, got {resp.status_code}"
        data = resp.json()
        assert data["name"] == "in_c_sales"
        assert data["table_count"] == 0

    def test_list_buckets(self):
        """Test listing buckets."""
        resp = self.client.get(f"/projects/{self.test_project_id}/buckets")
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["total"] == 1
        assert data["buckets"][0]["name"] == "in_c_sales"

    def test_get_bucket(self):
        """Test getting bucket."""
        resp = self.client.get(f"/projects/{self.test_project_id}/buckets/in_c_sales")
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["name"] == "in_c_sales"

    def test_create_table(self):
        """Test table creation (simple, no PK)."""
        resp = self.client.post(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables",
            json={
                "name": "customers",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "email", "type": "VARCHAR"},
                ],
            },
        )
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 201, f"Expected 201, got {resp.status_code}"
        data = resp.json()
        assert data["name"] == "customers"
        assert data["bucket"] == "in_c_sales"
        assert len(data["columns"]) == 3
        assert data["row_count"] == 0

    def test_create_table_with_pk(self):
        """Test table creation with primary key."""
        resp = self.client.post(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables",
            json={
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "customer_id", "type": "INTEGER"},
                    {"name": "amount", "type": "DOUBLE"},
                    {"name": "created_at", "type": "TIMESTAMP"},
                ],
                "primary_key": ["id"],
            },
        )
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 201, f"Expected 201, got {resp.status_code}"
        data = resp.json()
        assert data["name"] == "orders"
        assert "id" in data["primary_key"], f"Expected 'id' in PK, got {data['primary_key']}"

    def test_list_tables(self):
        """Test listing tables."""
        resp = self.client.get(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables"
        )
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["total"] == 2
        table_names = [t["name"] for t in data["tables"]]
        assert "customers" in table_names
        assert "orders" in table_names

    def test_get_table(self):
        """Test getting table (ObjectInfo)."""
        resp = self.client.get(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables/orders"
        )
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["name"] == "orders"
        assert data["bucket"] == "in_c_sales"
        assert len(data["columns"]) == 4

        # Check column details
        col_names = [c["name"] for c in data["columns"]]
        assert "id" in col_names
        assert "amount" in col_names

    def test_preview_empty(self):
        """Test preview of empty table."""
        resp = self.client.get(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables/orders/preview"
        )
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["total_row_count"] == 0
        assert data["preview_row_count"] == 0
        assert data["rows"] == []

    def test_insert_data(self):
        """Insert test data directly via DuckDB."""
        # This uses the database module directly to insert test data
        # In a real scenario, this would be done via Import endpoint
        import duckdb
        from pathlib import Path

        # Find the project DB file
        data_dir = Path("data/duckdb")
        db_path = data_dir / f"project_{self.test_project_id}.duckdb"

        if not db_path.exists():
            raise AssertionError(f"Project DB not found at {db_path}")

        conn = duckdb.connect(str(db_path))
        try:
            # Insert into customers
            conn.execute("""
                INSERT INTO in_c_sales.customers VALUES
                (1, 'Alice', 'alice@example.com'),
                (2, 'Bob', 'bob@example.com'),
                (3, 'Charlie', 'charlie@example.com')
            """)

            # Insert into orders
            conn.execute("""
                INSERT INTO in_c_sales.orders VALUES
                (1, 1, 99.99, '2024-01-15 10:30:00'),
                (2, 1, 149.50, '2024-01-16 14:20:00'),
                (3, 2, 250.00, '2024-01-17 09:00:00'),
                (4, 3, 75.25, '2024-01-18 16:45:00'),
                (5, 2, 199.99, '2024-01-19 11:30:00')
            """)
            conn.commit()

            # Verify
            count = conn.execute(
                "SELECT COUNT(*) FROM in_c_sales.orders"
            ).fetchone()[0]
            assert count == 5, f"Expected 5 rows, got {count}"

            self.log_verbose(f"Inserted 3 customers and 5 orders")
        finally:
            conn.close()

    def test_preview_with_data(self):
        """Test preview with data."""
        resp = self.client.get(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables/orders/preview"
        )
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["total_row_count"] == 5
        assert data["preview_row_count"] == 5
        assert len(data["rows"]) == 5

        # Check data structure
        row = data["rows"][0]
        assert "id" in row
        assert "amount" in row

    def test_preview_with_limit(self):
        """Test preview with custom limit."""
        resp = self.client.get(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables/orders/preview",
            params={"limit": 2},
        )
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["total_row_count"] == 5  # Total still 5
        assert data["preview_row_count"] == 2  # But only 2 returned
        assert len(data["rows"]) == 2

    def test_project_stats_with_data(self):
        """Test project stats with data."""
        resp = self.client.get(f"/projects/{self.test_project_id}/stats")
        self.log_verbose(f"Response: {resp.json()}")

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data["bucket_count"] == 1
        assert data["table_count"] == 2
        assert data["size_bytes"] > 0  # Should have some size now

    def test_delete_table(self):
        """Test table deletion."""
        # Delete customers table
        resp = self.client.delete(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables/customers"
        )
        assert resp.status_code == 204, f"Expected 204, got {resp.status_code}"

        # Verify deleted
        resp = self.client.get(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables/customers"
        )
        assert resp.status_code == 404, "Table should be deleted"

        # Delete orders table
        resp = self.client.delete(
            f"/projects/{self.test_project_id}/buckets/in_c_sales/tables/orders"
        )
        assert resp.status_code == 204, f"Expected 204, got {resp.status_code}"

        self.log_verbose("Both tables deleted")

    def test_delete_bucket(self):
        """Test bucket deletion."""
        resp = self.client.delete(
            f"/projects/{self.test_project_id}/buckets/in_c_sales"
        )
        assert resp.status_code == 204, f"Expected 204, got {resp.status_code}"

        # Verify deleted
        resp = self.client.get(
            f"/projects/{self.test_project_id}/buckets/in_c_sales"
        )
        assert resp.status_code == 404, "Bucket should be deleted"

        self.log_verbose("Bucket deleted")

    def test_delete_project(self):
        """Test project deletion (soft delete)."""
        resp = self.client.delete(f"/projects/{self.test_project_id}")
        assert resp.status_code == 204, f"Expected 204, got {resp.status_code}"

        # Verify status changed to deleted
        resp = self.client.get(f"/projects/{self.test_project_id}")
        assert resp.status_code == 200  # Still accessible
        data = resp.json()
        assert data["status"] == "deleted", f"Expected deleted, got {data['status']}"

        self.log_verbose("Project soft-deleted")


def main():
    parser = argparse.ArgumentParser(description="E2E tests for DuckDB API Service")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="Base URL of the API (default: http://localhost:8000)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose output"
    )
    args = parser.parse_args()

    runner = E2ETestRunner(args.base_url, args.verbose)

    try:
        success = runner.run_all()
        sys.exit(0 if success else 1)
    except httpx.ConnectError:
        print(f"\n{Colors.RED}ERROR: Cannot connect to {args.base_url}{Colors.END}")
        print("Make sure the server is running:")
        print("  cd duckdb-api-service")
        print("  source .venv/bin/activate")
        print("  python -m src.main")
        sys.exit(1)
    finally:
        runner.client.close()


if __name__ == "__main__":
    main()
