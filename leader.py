"""
Eleição de líder usando algoritmo em anel.
As mensagens circulam pelo anel repassando o maior ID visto.
"""

import socket
import threading
import logging

from proto import Packet, Kind

log = logging.getLogger(__name__)

_PROBE_INTERVAL = 4.0
_PROBE_TIMEOUT  = 2.0


class RingLeader:
    """Coordena eleição de líder por passagem de mensagem em anel."""

    def __init__(self, hub):
        self._hub         = hub
        self._in_election = False
        self._lock        = threading.Lock()

    # ── API pública ──────────────────────────────────────────────────────────

    def start_probe(self):
        """Inicia monitoramento periódico do líder em thread de fundo."""
        t = threading.Thread(target=self._probe_loop, daemon=True, name="probe")
        t.start()

    def start_election(self):
        """Dispara eleição em anel se não houver uma em andamento."""
        with self._lock:
            if self._in_election:
                return
            self._in_election = True

        print(f"\n[ELEIÇÃO] Nó {self._hub.node_id} iniciando eleição em anel...")
        self._forward(Packet(
            kind=Kind.ELECTION,
            src=self._hub.node_id,
            candidate=self._hub.node_id,
        ))

    # ── Handlers ─────────────────────────────────────────────────────────────

    def on_election(self, pkt: Packet):
        """Processa mensagem ELECTION recebida do nó anterior no anel."""
        if pkt.candidate == self._hub.node_id:
            # Minha mensagem completou o circuito — sou o maior ID vivo
            self._hub.leader_id = self._hub.node_id
            with self._lock:
                self._in_election = False
            print(f"\n[ELEIÇÃO] Nó {self._hub.node_id} concluiu o anel. Novo LÍDER.")
            self._hub.broadcast(Packet(
                kind=Kind.LEADER,
                src=self._hub.node_id,
                leader_id=self._hub.node_id,
            ))
            if self._hub.shell_ref:
                self._hub.shell_ref.refresh_prompt()

        elif pkt.candidate > self._hub.node_id:
            # Candidato maior — repassa sem alterar
            self._forward(pkt)

        else:
            # Candidato menor — substitui pelo próprio ID
            with self._lock:
                self._in_election = True
            self._forward(Packet(
                kind=Kind.ELECTION,
                src=self._hub.node_id,
                candidate=self._hub.node_id,
            ))

    def on_leader(self, pkt: Packet):
        """Recebe anúncio do novo líder e atualiza estado local."""
        self._hub.leader_id = pkt.leader_id
        with self._lock:
            self._in_election = False
        print(f"\n[ELEIÇÃO] Líder reconhecido: nó {pkt.leader_id}.")
        if self._hub.shell_ref:
            self._hub.shell_ref.refresh_prompt()

    # ── Internals ─────────────────────────────────────────────────────────────

    def _forward(self, pkt: Packet):
        """
        Encaminha o pacote para o próximo nó vivo no anel.
        Pula nós inacessíveis até encontrar um disponível.
        Se ninguém responder e o candidato for este nó, assume liderança.
        """
        ids    = sorted([p["id"] for p in self._hub.peers] + [self._hub.node_id])
        n      = len(ids)
        my_idx = ids.index(self._hub.node_id)

        for step in range(1, n):
            nxt_id = ids[(my_idx + step) % n]
            if nxt_id == self._hub.node_id:
                break
            peer = self._hub.peer_by_id(nxt_id)
            if peer and self._hub.send(peer, pkt):
                return

        # Nenhum próximo disponível: se sou o maior candidato, assumo a liderança
        if pkt.candidate == self._hub.node_id:
            log.info(f"[ELEIÇÃO] Anel sem próximo disponível; nó {self._hub.node_id} assume liderança.")
            self._hub.leader_id = self._hub.node_id
            with self._lock:
                self._in_election = False
            print(f"\n[ELEIÇÃO] Nó {self._hub.node_id} é o único nó vivo. Assumindo liderança.")
            if self._hub.shell_ref:
                self._hub.shell_ref.refresh_prompt()

    def _probe_loop(self):
        """Thread que verifica se o líder ainda está ativo."""
        import time
        time.sleep(_PROBE_INTERVAL)
        while self._hub.alive:
            time.sleep(_PROBE_INTERVAL)
            if self._hub.leader_id == self._hub.node_id:
                continue
            peer = self._hub.peer_by_id(self._hub.leader_id)
            if peer is None:
                self.start_election()
                continue
            if not self._ping(peer):
                print(f"\n[PROBE] Líder (nó {self._hub.leader_id}) inacessível. Iniciando eleição.")
                self.start_election()

    def _ping(self, peer: dict) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(_PROBE_TIMEOUT)
                s.connect((peer["address"], peer["port"]))
                s.sendall(Packet(kind=Kind.PING, src=self._hub.node_id).encode())
                return bool(s.recv(1024))
        except Exception:
            return False
