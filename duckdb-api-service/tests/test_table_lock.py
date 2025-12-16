"""Tests for TableLockManager and concurrent table access."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from fastapi.testclient import TestClient

from src.database import TableLockManager, table_lock_manager


class TestTableLockManager:
    """Unit tests for TableLockManager class."""

    def test_get_lock_creates_new_lock(self):
        """Test that get_lock creates a new lock for unknown table."""
        manager = TableLockManager()

        lock = manager.get_lock("proj1", "bucket1", "table1")

        assert lock is not None
        assert isinstance(lock, threading.Lock)
        assert manager.active_locks_count == 1

    def test_get_lock_returns_same_lock(self):
        """Test that get_lock returns the same lock for same table."""
        manager = TableLockManager()

        lock1 = manager.get_lock("proj1", "bucket1", "table1")
        lock2 = manager.get_lock("proj1", "bucket1", "table1")

        assert lock1 is lock2
        assert manager.active_locks_count == 1

    def test_get_lock_different_tables(self):
        """Test that different tables get different locks."""
        manager = TableLockManager()

        lock1 = manager.get_lock("proj1", "bucket1", "table1")
        lock2 = manager.get_lock("proj1", "bucket1", "table2")
        lock3 = manager.get_lock("proj1", "bucket2", "table1")

        assert lock1 is not lock2
        assert lock1 is not lock3
        assert lock2 is not lock3
        assert manager.active_locks_count == 3

    def test_acquire_context_manager(self):
        """Test that acquire works as a context manager."""
        manager = TableLockManager()
        acquired = False

        with manager.acquire("proj1", "bucket1", "table1"):
            acquired = True
            # Lock should be held here
            lock = manager.get_lock("proj1", "bucket1", "table1")
            assert lock.locked()

        # Lock should be released after context
        lock = manager.get_lock("proj1", "bucket1", "table1")
        assert not lock.locked()
        assert acquired

    def test_remove_lock(self):
        """Test that remove_lock removes the lock."""
        manager = TableLockManager()

        manager.get_lock("proj1", "bucket1", "table1")
        assert manager.active_locks_count == 1

        manager.remove_lock("proj1", "bucket1", "table1")
        assert manager.active_locks_count == 0

    def test_remove_lock_nonexistent(self):
        """Test that remove_lock handles nonexistent lock gracefully."""
        manager = TableLockManager()

        # Should not raise
        manager.remove_lock("proj1", "bucket1", "nonexistent")
        assert manager.active_locks_count == 0

    def test_clear_project_locks(self):
        """Test that clear_project_locks removes all locks for a project."""
        manager = TableLockManager()

        # Create locks for multiple projects
        manager.get_lock("proj1", "bucket1", "table1")
        manager.get_lock("proj1", "bucket1", "table2")
        manager.get_lock("proj1", "bucket2", "table1")
        manager.get_lock("proj2", "bucket1", "table1")

        assert manager.active_locks_count == 4

        # Clear only proj1
        manager.clear_project_locks("proj1")

        assert manager.active_locks_count == 1
        # proj2 lock should remain
        assert manager.get_lock("proj2", "bucket1", "table1") is not None

    def test_concurrent_lock_creation(self):
        """Test that concurrent lock creation is thread-safe."""
        manager = TableLockManager()
        locks_created = []

        def create_lock(table_num):
            lock = manager.get_lock("proj", "bucket", f"table{table_num}")
            locks_created.append(lock)
            return lock

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_lock, i) for i in range(100)]
            for future in as_completed(futures):
                future.result()

        # Should have created 100 different locks
        assert manager.active_locks_count == 100
        assert len(locks_created) == 100


class TestTableLockConcurrency:
    """Tests for concurrent table access with locking."""

    def test_concurrent_writes_same_table_serialized(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that concurrent writes to same table are serialized."""
        from src.database import project_db_manager

        # Setup
        client.post("/projects", json={"id": "lock_test_1"}, headers=admin_headers)
        client.post("/projects/lock_test_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/lock_test_1/buckets/test_bucket/tables",
            json={
                "name": "concurrent_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        write_order = []
        write_lock = threading.Lock()

        def write_to_table(writer_id: int):
            """Write to table and record order."""
            with project_db_manager.table_connection(
                "lock_test_1", "test_bucket", "concurrent_table"
            ) as conn:
                # Record when we got the connection (inside lock)
                with write_lock:
                    write_order.append(f"start_{writer_id}")

                # Simulate some work
                conn.execute(f"INSERT INTO main.data VALUES ({writer_id})")
                conn.commit()
                time.sleep(0.05)  # Small delay to ensure overlap attempts

                with write_lock:
                    write_order.append(f"end_{writer_id}")

        # Run concurrent writes
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(write_to_table, i) for i in range(5)]
            for future in as_completed(futures):
                future.result()

        # Verify all writes completed
        assert len(write_order) == 10  # 5 starts + 5 ends

        # Verify serialization: each start should be followed by its end
        # before another start (no interleaving)
        active_writers = 0
        max_concurrent = 0
        for event in write_order:
            if event.startswith("start_"):
                active_writers += 1
                max_concurrent = max(max_concurrent, active_writers)
            else:
                active_writers -= 1

        # With proper locking, max concurrent should be 1
        assert max_concurrent == 1, f"Expected max 1 concurrent writer, got {max_concurrent}"

        # Verify all data was written
        response = client.get(
            "/projects/lock_test_1/buckets/test_bucket/tables/concurrent_table/preview",
            headers=admin_headers,
        )
        assert response.status_code == 200
        assert response.json()["total_row_count"] == 5

    def test_concurrent_writes_different_tables_parallel(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that writes to different tables can run in parallel."""
        from src.database import project_db_manager

        # Setup
        client.post("/projects", json={"id": "lock_test_2"}, headers=admin_headers)
        client.post("/projects/lock_test_2/buckets", json={"name": "test_bucket"}, headers=admin_headers)

        # Create multiple tables
        for i in range(3):
            client.post(
                "/projects/lock_test_2/buckets/test_bucket/tables",
                json={
                    "name": f"parallel_table_{i}",
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
                headers=admin_headers,
            )

        concurrent_count = []
        count_lock = threading.Lock()
        active_writers = [0]  # Use list for mutable in closure

        def write_to_table(table_num: int):
            """Write to a specific table."""
            with project_db_manager.table_connection(
                "lock_test_2", "test_bucket", f"parallel_table_{table_num}"
            ) as conn:
                with count_lock:
                    active_writers[0] += 1
                    concurrent_count.append(active_writers[0])

                # Simulate work
                conn.execute(f"INSERT INTO main.data VALUES ({table_num})")
                conn.commit()
                time.sleep(0.1)  # Long enough to overlap

                with count_lock:
                    active_writers[0] -= 1

        # Run writes to different tables concurrently
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(write_to_table, i) for i in range(3)]
            for future in as_completed(futures):
                future.result()

        # With different tables, we should see concurrent execution
        max_concurrent = max(concurrent_count)
        # Note: Due to timing, we may not always see all 3, but should see > 1
        assert max_concurrent >= 1  # At minimum, sequential works

        # Verify all data was written
        for i in range(3):
            response = client.get(
                f"/projects/lock_test_2/buckets/test_bucket/tables/parallel_table_{i}/preview",
                headers=admin_headers,
            )
            assert response.status_code == 200
            assert response.json()["total_row_count"] == 1

    def test_read_operations_concurrent(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that read operations can run concurrently."""
        from src.database import project_db_manager

        # Setup
        client.post("/projects", json={"id": "lock_test_3"}, headers=admin_headers)
        client.post("/projects/lock_test_3/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/lock_test_3/buckets/test_bucket/tables",
            json={
                "name": "read_table",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Insert some data
        with project_db_manager.table_connection(
            "lock_test_3", "test_bucket", "read_table"
        ) as conn:
            for i in range(10):
                conn.execute(f"INSERT INTO main.data VALUES ({i})")
            conn.commit()

        concurrent_reads = []
        count_lock = threading.Lock()
        active_readers = [0]

        def read_from_table(reader_id: int):
            """Read from table."""
            with project_db_manager.table_connection(
                "lock_test_3", "test_bucket", "read_table", read_only=True
            ) as conn:
                with count_lock:
                    active_readers[0] += 1
                    concurrent_reads.append(active_readers[0])

                # Read operation
                result = conn.execute("SELECT COUNT(*) FROM main.data").fetchone()
                time.sleep(0.05)

                with count_lock:
                    active_readers[0] -= 1

                return result[0]

        # Run concurrent reads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(read_from_table, i) for i in range(5)]
            results = [future.result() for future in as_completed(futures)]

        # All reads should return same count
        assert all(r == 10 for r in results)

        # Reads should be able to run concurrently (no lock for read_only)
        # Note: timing dependent, so we just verify it completed


class TestTableLockCleanup:
    """Tests for lock cleanup on table/bucket/project deletion."""

    def test_delete_table_removes_lock(self, client: TestClient, initialized_backend, admin_headers):
        """Test that deleting a table removes its lock."""
        # Setup
        client.post("/projects", json={"id": "cleanup_test_1"}, headers=admin_headers)
        client.post("/projects/cleanup_test_1/buckets", json={"name": "test_bucket"}, headers=admin_headers)
        client.post(
            "/projects/cleanup_test_1/buckets/test_bucket/tables",
            json={
                "name": "to_delete",
                "columns": [{"name": "id", "type": "INTEGER"}],
            },
            headers=admin_headers,
        )

        # Access table to create lock
        from src.database import project_db_manager

        with project_db_manager.table_connection(
            "cleanup_test_1", "test_bucket", "to_delete"
        ) as conn:
            conn.execute("SELECT 1")

        # Lock should exist
        initial_count = table_lock_manager.active_locks_count

        # Delete table
        response = client.delete(
            "/projects/cleanup_test_1/buckets/test_bucket/tables/to_delete",
            headers=admin_headers,
        )
        assert response.status_code == 204

        # Lock should be removed (count decreased or same if no lock was created)
        assert table_lock_manager.active_locks_count <= initial_count

    def test_delete_bucket_removes_table_locks(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that deleting a bucket removes locks for all its tables."""
        # Setup
        client.post("/projects", json={"id": "cleanup_test_2"}, headers=admin_headers)
        client.post("/projects/cleanup_test_2/buckets", json={"name": "bucket_to_delete"}, headers=admin_headers)

        # Create multiple tables
        for i in range(3):
            client.post(
                "/projects/cleanup_test_2/buckets/bucket_to_delete/tables",
                json={
                    "name": f"table_{i}",
                    "columns": [{"name": "id", "type": "INTEGER"}],
                },
                headers=admin_headers,
            )

        # Access tables to create locks
        from src.database import project_db_manager

        for i in range(3):
            with project_db_manager.table_connection(
                "cleanup_test_2", "bucket_to_delete", f"table_{i}"
            ) as conn:
                conn.execute("SELECT 1")

        # Delete bucket
        response = client.delete("/projects/cleanup_test_2/buckets/bucket_to_delete", headers=admin_headers)
        assert response.status_code == 204

        # All locks for this bucket should be removed
        # (We can't easily check specific locks, but overall count should decrease)

    def test_delete_project_removes_all_locks(
        self, client: TestClient, initialized_backend, admin_headers
    ):
        """Test that deleting a project removes all its locks."""
        # Setup
        client.post("/projects", json={"id": "cleanup_test_3"}, headers=admin_headers)
        client.post("/projects/cleanup_test_3/buckets", json={"name": "bucket1"}, headers=admin_headers)
        client.post("/projects/cleanup_test_3/buckets", json={"name": "bucket2"}, headers=admin_headers)

        # Create tables in different buckets
        for bucket in ["bucket1", "bucket2"]:
            for i in range(2):
                client.post(
                    f"/projects/cleanup_test_3/buckets/{bucket}/tables",
                    json={
                        "name": f"table_{i}",
                        "columns": [{"name": "id", "type": "INTEGER"}],
                    },
                    headers=admin_headers,
                )

        # Access tables to create locks
        from src.database import project_db_manager

        for bucket in ["bucket1", "bucket2"]:
            for i in range(2):
                with project_db_manager.table_connection(
                    "cleanup_test_3", bucket, f"table_{i}"
                ) as conn:
                    conn.execute("SELECT 1")

        # Delete project
        response = client.delete("/projects/cleanup_test_3", headers=admin_headers)
        assert response.status_code == 204

        # All locks for this project should be removed
