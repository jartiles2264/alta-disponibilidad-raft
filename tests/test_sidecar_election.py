"""Pruebas de la elección y el mantenimiento del liderazgo del sidecar.

Estas rutas son imposibles de ejercitar a mano en el laboratorio: exigen
que un nodo esté por detrás en GTID, que otro tenga historial divergente,
o que el servicio MySQL del primario se reinicie en el momento justo.
Se cubren aquí con dobles.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

import sidecar

LOCAL = "192.168.200.222"
OTHER = "192.168.206.205"


class FakeConnection:
    """Conexión mínima: choose_freshest_candidate solo la abre y cierra."""

    def close(self):
        pass


def _state(gtid, read_only=0, super_read_only=0, replica=None):
    return {
        "server_id": 1,
        "read_only": read_only,
        "super_read_only": super_read_only,
        "gtid_executed": gtid,
        "replica": replica,
    }


class GtidDeferralTests(unittest.TestCase):
    """El nodo cede ante uno más fresco, pero no indefinidamente.

    choose_freshest_candidate solo comprueba que el MySQL del otro nodo
    responda, no que su sidecar esté vivo. Sin un límite, un sidecar
    caído en el nodo más actualizado dejaría al clúster sin maestro para
    siempre: todos le ceden el paso y nadie reclama la clave.
    """

    def setUp(self):
        self._node_ip = sidecar.NODE_IP
        self._nodes = sidecar.MYSQL_NODES
        sidecar.NODE_IP = LOCAL
        sidecar.MYSQL_NODES = [LOCAL, OTHER]
        sidecar._deferrals = 0
        # Por defecto: el local está por detrás del remoto.
        self.relation = "behind"

        def fake_state(host=None):
            gtid = "local" if host in (LOCAL, sidecar.MYSQL_HOST) else "other"
            return _state(gtid)

        def fake_subset(_connection, left, right):
            if self.relation == "equal":
                return True
            if self.relation == "divergent":
                return False
            return left == "local" and right == "other"

        patches = [
            patch.object(sidecar, "get_mysql_state", fake_state),
            patch.object(sidecar, "get_db_connection", lambda host=None: FakeConnection()),
            patch.object(sidecar, "_gtid_subset", fake_subset),
        ]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)

    def tearDown(self):
        sidecar.NODE_IP = self._node_ip
        sidecar.MYSQL_NODES = self._nodes
        sidecar._deferrals = 0

    def test_cede_exactamente_max_deferrals_y_luego_compite(self):
        for ciclo in range(sidecar.MAX_DEFERRALS):
            self.assertIsNone(
                sidecar.choose_freshest_candidate(),
                f"debía ceder en el ciclo {ciclo + 1}",
            )
        self.assertEqual(
            sidecar.choose_freshest_candidate(),
            LOCAL,
            "tras agotar las cesiones debe competir",
        )
        self.assertEqual(sidecar._deferrals, 0, "el contador debe reiniciarse")

    def test_historial_divergente_nunca_compite(self):
        self.relation = "divergent"
        for _ in range(sidecar.MAX_DEFERRALS * 3):
            self.assertIsNone(sidecar.choose_freshest_candidate())
        self.assertEqual(
            sidecar._deferrals,
            0,
            "la divergencia no debe acumular cesiones: promover perdería "
            "transacciones confirmadas y eso no se salta nunca",
        )

    def test_gtid_iguales_compiten_de_inmediato(self):
        self.relation = "equal"
        self.assertEqual(sidecar.choose_freshest_candidate(), LOCAL)
        self.assertEqual(sidecar._deferrals, 0)

    def test_el_contador_se_reinicia_al_dejar_de_ceder(self):
        sidecar.choose_freshest_candidate()
        sidecar.choose_freshest_candidate()
        self.assertGreater(sidecar._deferrals, 0)
        self.relation = "equal"
        self.assertEqual(sidecar.choose_freshest_candidate(), LOCAL)
        self.assertEqual(sidecar._deferrals, 0)


class PrimaryWritabilityTests(unittest.TestCase):
    """El primario no puede quedarse en solo lectura conservando el lease.

    my.ini deja read_only=ON en el arranque a propósito, para que el viejo
    maestro reingrese como réplica tras el sabotaje. El efecto secundario
    es que un reinicio del servicio MySQL en el primario lo devuelve a
    solo lectura sin que pierda el liderazgo: etcd sigue publicando su IP
    y la aplicación sigue escribiendo contra él, fallando siempre.
    """

    def setUp(self):
        self._node_ip = sidecar.NODE_IP
        sidecar.NODE_IP = LOCAL
        self.addCleanup(lambda: setattr(sidecar, "NODE_IP", self._node_ip))

    def _run_hold(self, states):
        """Ejecuta hold_leadership hasta que el lease deje de refrescarse."""
        lease = MagicMock()
        # La tercera llamada corta el bucle (1 antes del bucle, 2 dentro del bucle).
        lease.refresh.side_effect = [None, None, RuntimeError("lease perdido")]
        client = MagicMock()
        client.get.return_value = (LOCAL.encode("utf-8"), None)

        with patch.object(sidecar, "promote_to_primary") as promote, \
             patch.object(sidecar, "fence_local_mysql") as fence, \
             patch.object(sidecar, "get_mysql_state", side_effect=states), \
             patch.object(sidecar.time, "sleep"):
            sidecar.hold_leadership(client, lease)
        return promote, fence

    def test_reabre_escrituras_si_mysql_volvio_a_solo_lectura(self):
        promote, fence = self._run_hold(
            [
                _state("g", read_only=1, super_read_only=1),  # tras reinicio
                _state("g"),
            ]
        )
        self.assertEqual(
            promote.call_count,
            2,
            "debe promover al empezar y volver a promover al detectar "
            "que MySQL regresó a solo lectura",
        )
        fence.assert_called_once()  # solo al perder el lease, no antes

    def test_no_repromueve_si_sigue_escribible(self):
        promote, _ = self._run_hold([_state("g"), _state("g")])
        self.assertEqual(
            promote.call_count,
            1,
            "sin cambio de estado no debe reafirmar nada",
        )

    def test_hace_fencing_al_perder_el_lease(self):
        _, fence = self._run_hold([_state("g"), _state("g")])
        fence.assert_called_once()
        self.assertIn("lease", fence.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
