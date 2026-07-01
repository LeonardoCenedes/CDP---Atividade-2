"""
Broadcast de mensagens ordenado usando relógio de Lamport.
As mensagens ficam numa fila até que seja seguro entregar.
"""

import heapq
import threading
import time
import logging
from typing import Callable

from proto import Packet, Kind

log = logging.getLogger(__name__)


class LamportBroadcast:
    """Multicast com ordem total garantida pelo Relógio de Lamport."""

    def __init__(self, hub, apply_fn: Callable[[str, str], None]):
        """
        Args:
            hub:      instância de Hub.
            apply_fn: callback(payload, descricao_origem) para aplicar
                      a operação no sistema de arquivos virtual.
        """
        self._hub    = hub
        self._apply  = apply_fn
        self._clock  = 0

        # min-heap: (lamport_ts, src_id, payload, arrival_wall_time)
        self._queue: list[tuple] = []

        # Maior clock recebido de cada peer conhecido
        self._latest: dict[int, int] = {p["id"]: 0 for p in hub.peers}
        self._lock = threading.Lock()

        # Thread periódica para entregar mensagens represadas
        threading.Thread(
            target=self._flush_loop, daemon=True, name="bcast-flush"
        ).start()

    # ── API pública ──────────────────────────────────────────────────────────

    def send(self, payload: str):
        """
        Envia uma operação para todos os nós com timestamp de Lamport.
        Aplica localmente antes de transmitir (o remetente já entregou).
        """
        with self._lock:
            self._clock += 1
            ts = self._clock

        self._apply(payload, f"local (t={ts})")

        pkt = Packet(
            kind=Kind.BCAST,
            src=self._hub.node_id,
            clock=ts,
            payload=payload,
        )
        self._hub.broadcast(pkt)
        log.debug(f"[BCAST] Enviado t={ts} payload='{payload}'")

    def on_receive(self, pkt: Packet):
        """Recebe BCAST remoto, atualiza relógio e enfileira para entrega."""
        with self._lock:
            # Regra de atualização do Relógio de Lamport no recebimento
            self._clock = max(self._clock, pkt.clock) + 1
            # Registra o maior clock já visto deste remetente
            prev = self._latest.get(pkt.src, 0)
            self._latest[pkt.src] = max(prev, pkt.clock)

            heapq.heappush(self._queue, (pkt.clock, pkt.src, pkt.payload, time.time()))
            log.debug(f"[BCAST] Recebido t={pkt.clock} de nó {pkt.src}. Fila={len(self._queue)}")
            self._try_deliver()

    # ── Internals ─────────────────────────────────────────────────────────────

    def _try_deliver(self):
        """
        Entrega mensagens da fila em ordem (ts, src) quando seguro.
        Deve ser chamado com self._lock adquirido.
        """
        while self._queue:
            ts, src, payload, arrival = self._queue[0]

            # Condição Lamport-safe: todos os peers têm clock registrado >= ts
            safe = all(
                self._latest.get(pid, 0) >= ts
                for pid in [p["id"] for p in self._hub.peers]
            )
            # Fallback: entrega forçada após 2s para evitar starvation
            forced = (time.time() - arrival) > 2.0

            if safe or forced or not self._hub.peers:
                heapq.heappop(self._queue)
                tag = " (forçado)" if forced and not safe else ""
                log.debug(f"[BCAST] Entregando t={ts} de nó {src}{tag}.")
                self._apply(payload, f"nó {src} (t={ts}){tag}")
            else:
                break

    def _flush_loop(self):
        """Verifica periodicamente entregas represadas na fila."""
        while self._hub.alive:
            time.sleep(0.5)
            with self._lock:
                self._try_deliver()
