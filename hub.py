"""
Camada de rede: servidor TCP de escuta e envio de pacotes.
Roteia mensagens recebidas para os módulos de protocolo adequados.
"""

import socket
import threading
import logging

from proto import Packet, Kind

log = logging.getLogger(__name__)
_BUF = 65536


class Hub:
    """Núcleo de comunicação do nó distribuído."""

    def __init__(self, node_id: int, address: str, port: int, peers: list[dict]):
        self.node_id   = node_id
        self.address   = address
        self.port      = port
        self.peers     = peers
        self.leader_id = max([p["id"] for p in peers] + [node_id])
        self.alive     = True
        self.shell_ref = None

        # Módulos injetados após a construção (em main.py)
        self.mutex        = None
        self.election_mgr = None
        self.bcast_mgr    = None

    # ── Servidor ──────────────────────────────────────────────────────────────

    def start(self):
        t = threading.Thread(
            target=self._listen, daemon=True, name=f"hub-{self.node_id}"
        )
        t.start()
        log.info(f"[HUB] Aguardando conexões em {self.address}:{self.port}")

    def _listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.address, self.port))
            srv.listen(16)
            srv.settimeout(1.0)
            while self.alive:
                try:
                    conn, _ = srv.accept()
                    threading.Thread(
                        target=self._handle, args=(conn,), daemon=True
                    ).start()
                except socket.timeout:
                    continue
                except OSError:
                    break

    def _handle(self, conn: socket.socket):
        try:
            with conn:
                buf = b""
                while True:
                    chunk = conn.recv(_BUF)
                    if not chunk:
                        break
                    buf += chunk
                    if b"\n" in buf:
                        break
                if not buf:
                    return
                pkt = Packet.decode(buf.decode("utf-8").strip())
                self._route(pkt, conn)
        except Exception as e:
            log.debug(f"[HUB] Erro na conexão: {e}")

    def _route(self, pkt: Packet, conn: socket.socket):
        if pkt.kind == Kind.PING:
            conn.sendall(Packet(kind=Kind.PONG, src=self.node_id).encode())

        elif pkt.kind == Kind.REQUEST:
            if self.mutex:
                self.mutex.on_request(pkt)

        elif pkt.kind == Kind.GRANT:
            if self.mutex:
                self.mutex.on_grant(pkt)

        elif pkt.kind == Kind.ELECTION:
            if self.election_mgr:
                self.election_mgr.on_election(pkt)

        elif pkt.kind == Kind.LEADER:
            if self.election_mgr:
                self.election_mgr.on_leader(pkt)

        elif pkt.kind == Kind.BCAST:
            if self.bcast_mgr:
                self.bcast_mgr.on_receive(pkt)

        else:
            log.warning(f"[HUB] Pacote desconhecido: {pkt.kind}")

    # ── Envio ─────────────────────────────────────────────────────────────────

    def send(self, peer: dict, pkt: Packet) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2.0)
                s.connect((peer["address"], peer["port"]))
                s.sendall(pkt.encode())
            return True
        except Exception as e:
            log.debug(f"[HUB] Falha ao alcançar nó {peer['id']}: {e}")
            return False

    def broadcast(self, pkt: Packet):
        for peer in self.peers:
            self.send(peer, pkt)

    def peer_by_id(self, nid: int) -> dict | None:
        return next((p for p in self.peers if p["id"] == nid), None)

    # ── Ciclo de vida ─────────────────────────────────────────────────────────

    def crash(self):
        print(f"\n[!] Nó {self.node_id} simulando falha. Encerrando participação na rede.")
        self.alive = False
