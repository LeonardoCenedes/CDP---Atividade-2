"""
Exclusão mútua usando Ricart-Agrawala.
Garante acesso a seção crítica por meio de votação distribuída.
"""

import threading
import logging

from proto import Packet, Kind

log = logging.getLogger(__name__)

_RELEASED = 0
_WANTED   = 1
_HELD     = 2


class RicartAgrawala:
    """Controle de acesso exclusivo por votação distribuída."""

    def __init__(self, hub):
        self._hub          = hub
        self._state        = _RELEASED
        self._lamport      = 0
        self._req_ts       = 0
        self._deferred: list[int] = []
        self._grants_recv  = 0
        self._needed       = len(hub.peers)
        self._resource: str | None = None
        self._lock         = threading.Lock()
        self._cs_ready     = threading.Event()

    # ── API pública ──────────────────────────────────────────────────────────

    def lock(self, resource: str) -> bool:
        """
        Solicita acesso exclusivo ao recurso indicado.
        Bloqueia até receber permissão de todos os outros nós (ou timeout).
        """
        with self._lock:
            if self._state != _RELEASED:
                print(f"[RA] Pedido já em andamento para '{self._resource}'.")
                return False
            self._lamport   += 1
            self._req_ts     = self._lamport
            self._grants_recv = 0
            self._state      = _WANTED
            self._resource   = resource

        if self._needed == 0:
            with self._lock:
                self._state = _HELD
            return True

        print(f"[RA] Enviando REQUEST para '{resource}' (t={self._req_ts})...")
        pkt = Packet(
            kind=Kind.REQUEST,
            src=self._hub.node_id,
            clock=self._req_ts,
            resource=resource,
        )
        self._hub.broadcast(pkt)

        ok = self._cs_ready.wait(timeout=30.0)
        self._cs_ready.clear()
        if not ok:
            with self._lock:
                self._state   = _RELEASED
                self._resource = None
        return ok

    def unlock(self):
        """Libera a SC e envia GRANT para todos os nós com pedidos adiados."""
        with self._lock:
            if self._state != _HELD:
                print("[RA] Nenhuma seção crítica ativa no momento.")
                return
            self._state    = _RELEASED
            deferred       = list(self._deferred)
            self._deferred.clear()
            resource       = self._resource or ""
            self._resource = None

        print(f"[RA] Recurso liberado. Notificando {len(deferred)} nó(s) na fila.")
        for nid in deferred:
            peer = self._hub.peer_by_id(nid)
            if peer:
                self._hub.send(peer, Packet(
                    kind=Kind.GRANT,
                    src=self._hub.node_id,
                    resource=resource,
                ))

    @property
    def has_permission(self) -> bool:
        """True se este nó está atualmente na seção crítica."""
        return self._state == _HELD

    # ── Handlers ─────────────────────────────────────────────────────────────

    def on_request(self, pkt: Packet):
        """
        Processa REQUEST de outro nó.
        Decide entre conceder GRANT agora ou adiar para depois.
        """
        with self._lock:
            self._lamport = max(self._lamport, pkt.clock) + 1

            # Este nó tem prioridade se está WANTED com timestamp menor
            i_have_priority = (
                self._state == _WANTED
                and (self._req_ts, self._hub.node_id) < (pkt.clock, pkt.src)
            )

            if self._state == _HELD or i_have_priority:
                self._deferred.append(pkt.src)
                log.debug(f"[RA] GRANT adiado para nó {pkt.src}.")
                return

        peer = self._hub.peer_by_id(pkt.src)
        if peer:
            self._hub.send(peer, Packet(
                kind=Kind.GRANT,
                src=self._hub.node_id,
                resource=pkt.resource,
            ))
            log.debug(f"[RA] GRANT enviado para nó {pkt.src}.")

    def on_grant(self, pkt: Packet):
        """Registra um GRANT recebido; entra na SC ao atingir quórum completo."""
        with self._lock:
            self._lamport     = max(self._lamport, pkt.clock) + 1
            self._grants_recv += 1
            if self._grants_recv >= self._needed:
                self._state = _HELD
                self._cs_ready.set()
                log.debug(f"[RA] Quórum atingido ({self._grants_recv}/{self._needed}).")
