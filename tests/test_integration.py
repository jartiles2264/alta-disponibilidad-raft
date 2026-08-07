import queue
import unittest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import banco_app
import db_pool
import etcd_watcher
import sidecar


class EndpointContractTests(unittest.TestCase):
    def test_sidecar_parses_multiple_endpoints(self):
        self.assertEqual(
            sidecar.parse_endpoints("node1:2379,node2:2380"),
            [("node1", 2379), ("node2", 2380)],
        )

    def test_watcher_rejects_empty_endpoint_list(self):
        with self.assertRaises(ValueError):
            etcd_watcher._parse_endpoints(" , ")

    def test_watcher_validates_primary_ip(self):
        self.assertEqual(
            etcd_watcher._decode_master_ip(b"192.168.200.222"),
            "192.168.200.222",
        )
        with self.assertRaises(ValueError):
            etcd_watcher._decode_master_ip(b"")


class PoolStateTests(unittest.TestCase):
    def setUp(self):
        self.manager = db_pool.DynamicDatabasePool()
        self.manager._password = "test-only"

    @patch("db_pool.mysql.connector.pooling.MySQLConnectionPool")
    def test_pool_is_published_only_after_success(self, pool_factory):
        fake_pool = MagicMock()
        pool_factory.return_value = fake_pool

        self.manager.initialize_pool("192.168.200.222")

        self.assertIs(self.manager.pool, fake_pool)
        self.assertEqual(self.manager.current_ip, "192.168.200.222")
        self.assertTrue(self.manager.pool_ready.is_set())

    @patch("db_pool.mysql.connector.pooling.MySQLConnectionPool")
    def test_failed_pool_remains_unavailable(self, pool_factory):
        pool_factory.side_effect = db_pool.mysql.connector.Error("offline")

        with self.assertRaises(db_pool.mysql.connector.Error):
            self.manager.initialize_pool("192.168.206.205")

        self.assertIsNone(self.manager.pool)
        self.assertIsNone(self.manager.current_ip)
        self.assertFalse(self.manager.pool_ready.is_set())

    @patch("db_pool.mysql.connector.pooling.MySQLConnectionPool")
    def test_pool_requires_password_from_environment(self, pool_factory):
        self.manager._password = ""
        with self.assertRaises(RuntimeError):
            self.manager.initialize_pool("192.168.200.222")
        pool_factory.assert_not_called()

    def test_invalidate_pool_closes_old_connections(self):
        connection = MagicMock()
        fake_pool = MagicMock()
        fake_pool._cnx_queue = queue.Queue()
        fake_pool._cnx_queue.put(connection)
        self.manager.pool = fake_pool
        self.manager.current_ip = "192.168.200.222"
        self.manager.pool_ready.set()

        self.manager.invalidate_pool()

        connection.close.assert_called_once()
        self.assertIsNone(self.manager.pool)
        self.assertIsNone(self.manager.current_ip)
        self.assertFalse(self.manager.pool_ready.is_set())

    def test_get_connection_is_blocked_during_failover(self):
        self.manager.pool = MagicMock()
        self.manager.pool_ready.clear()
        with self.assertRaises(RuntimeError):
            self.manager.get_connection()


class SidecarCandidateTests(unittest.TestCase):
    def test_writable_node_is_eligible(self):
        state = {"read_only": 0, "super_read_only": 0, "replica": None}
        self.assertTrue(sidecar.is_healthy_candidate(state))

    def test_replica_requires_healthy_sql_thread(self):
        healthy = {
            "read_only": 1,
            "super_read_only": 1,
            "replica": {
                "Replica_SQL_Running": "Yes",
                "Last_SQL_Errno": 0,
            },
        }
        broken = {
            "read_only": 1,
            "super_read_only": 1,
            "replica": {
                "Replica_SQL_Running": "No",
                "Last_SQL_Errno": 1062,
            },
        }
        self.assertTrue(sidecar.is_healthy_candidate(healthy))
        self.assertFalse(sidecar.is_healthy_candidate(broken))

    def test_equally_fresh_nodes_are_allowed_to_compete(self):
        state = {
            "read_only": 1,
            "super_read_only": 1,
            "gtid_executed": "uuid:1-10",
            "replica": {
                "Replica_SQL_Running": "Yes",
                "Last_SQL_Errno": 0,
            },
        }
        connection = MagicMock()
        with (
            patch.object(
                sidecar, "MYSQL_NODES", ["10.0.0.1", "10.0.0.2"]
            ),
            patch.object(sidecar, "NODE_IP", "10.0.0.1"),
            patch.object(sidecar, "MYSQL_HOST", "127.0.0.1"),
            patch.object(sidecar, "get_mysql_state", return_value=state),
            patch.object(sidecar, "get_db_connection", return_value=connection),
            patch.object(sidecar, "_gtid_subset", side_effect=[True, True]),
        ):
            self.assertEqual(
                sidecar.choose_freshest_candidate(), "10.0.0.1"
            )
        connection.close.assert_called_once()

    def test_node_behind_gtid_is_rejected(self):
        state = {
            "read_only": 1,
            "super_read_only": 1,
            "gtid_executed": "uuid:1-5",
            "replica": {
                "Replica_SQL_Running": "Yes",
                "Last_SQL_Errno": 0,
            },
        }
        connection = MagicMock()
        with (
            patch.object(
                sidecar, "MYSQL_NODES", ["10.0.0.1", "10.0.0.2"]
            ),
            patch.object(sidecar, "NODE_IP", "10.0.0.1"),
            patch.object(sidecar, "MYSQL_HOST", "127.0.0.1"),
            patch.object(sidecar, "get_mysql_state", return_value=state),
            patch.object(sidecar, "get_db_connection", return_value=connection),
            patch.object(sidecar, "_gtid_subset", side_effect=[True, False]),
        ):
            self.assertIsNone(sidecar.choose_freshest_candidate())


class BankingValidationTests(unittest.TestCase):
    @patch("banco_app.DynamicDatabasePool.get_instance")
    def test_invalid_transfers_never_open_the_pool(self, get_instance):
        self.assertFalse(banco_app.transferir_dinero("1111", "2222", -1))
        self.assertFalse(
            banco_app.transferir_dinero("1111", "2222", float("nan"))
        )
        self.assertFalse(banco_app.transferir_dinero("1111", "1111", 1))
        get_instance.assert_not_called()


if __name__ == "__main__":
    unittest.main()
