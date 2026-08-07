#!/usr/bin/env python3
"""End-to-end cluster tests.

These tests require the Docker cluster to be running:
    docker compose up -d --build

Run with:
    python tests/test_cluster_e2e.py
"""

import subprocess
import sys
import time


def run(cmd, capture=True):
    """Run a shell command and return stdout."""
    result = subprocess.run(
        cmd, shell=True, capture_output=capture, text=True, timeout=30
    )
    if result.returncode != 0 and capture:
        print(f"  STDERR: {result.stderr.strip()}")
    return result.stdout.strip() if capture else ""


def test_etcd_health():
    """Verify all 3 etcd nodes are healthy."""
    print("\n=== Test: etcd cluster health ===")
    for node in ["etcd-1", "etcd-2", "etcd-3"]:
        output = run(
            f"docker exec {node} etcdctl endpoint health "
            f"--endpoints=http://localhost:2379"
        )
        assert "is healthy" in output, f"{node} is not healthy: {output}"
        print(f"  ✓ {node} is healthy")


def test_mysql_health():
    """Verify all 5 MySQL nodes are running."""
    print("\n=== Test: MySQL nodes health ===")
    for i in range(1, 6):
        node = f"mysql-node-{i}"
        output = run(
            f"docker exec {node} mysqladmin ping "
            f"-uroot -prootpassword --silent"
        )
        assert "alive" in output.lower() or output == "mysqld is alive", \
            f"{node} is not alive: {output}"
        print(f"  ✓ {node} is alive")


def test_primary_elected():
    """Verify a primary has been elected in etcd."""
    print("\n=== Test: Primary election ===")
    primary = run(
        "docker exec etcd-1 etcdctl get /banco/mysql/primary "
        "--print-value-only"
    )
    assert primary, "No primary was elected"
    print(f"  ✓ Primary elected: {primary}")

def test_failover():
    """Test automatic failover by stopping the primary."""
    print("\n=== Test: Automatic failover ===")

    # Get current primary
    primary = run(
        "docker exec etcd-1 etcdctl get /banco/mysql/primary "
        "--print-value-only"
    )
    assert primary, "No primary was elected"

    # Stop primary MySQL and its sidecar
    print("  Stopping mysql-node-1 and sidecar-1...")
    run("docker compose stop mysql-node-1 sidecar-1",
        capture=False)

    # Wait for failover
    print("  Waiting for failover (20s)...")
    time.sleep(20)

    # Verify new primary
    new_primary = run(
        "docker exec etcd-2 etcdctl get /banco/mysql/primary "
        "--print-value-only"
    )
    assert new_primary, "No new primary after failover"
    assert new_primary != primary, \
        f"Primary did not change: still {primary}"
    print(f"  ✓ Failover successful: {primary} → {new_primary}")

    # Restart stopped services
    print("  Restarting mysql-node-1 and sidecar-1...")
    run("docker compose start mysql-node-1 sidecar-1",
        capture=False)
    time.sleep(10)
    print("  ✓ Services restarted")


def main():
    print("╔══════════════════════════════════════════╗")
    print("║  Cluster E2E Tests                       ║")
    print("╚══════════════════════════════════════════╝")

    try:
        test_etcd_health()
        test_mysql_health()
        test_primary_elected()
        test_failover()
        print("\n✅ All tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
