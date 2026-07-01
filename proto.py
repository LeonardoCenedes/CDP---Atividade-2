"""
Definição do protocolo de comunicação entre nós do cluster.
Cada mensagem é serializada em JSON antes de ser enviada via TCP.
"""

from dataclasses import dataclass, asdict
from enum import Enum
import json


class Kind(str, Enum):
    REQUEST  = "REQUEST"   # Ricart-Agrawala: pedido de acesso à SC
    GRANT    = "GRANT"     # Ricart-Agrawala: permissão concedida
    ELECTION = "ELECTION"  # Eleição em anel: candidato circulando
    LEADER   = "LEADER"    # Eleição em anel: anúncio do vencedor
    BCAST    = "BCAST"     # Multicast Lamport: propagação de operação
    PING     = "PING"      # Verificação de disponibilidade
    PONG     = "PONG"      # Resposta ao PING


@dataclass
class Packet:
    """Unidade de comunicação entre nós."""
    kind: str
    src: int
    clock: int = 0        # Relógio de Lamport (REQUEST, BCAST)
    candidate: int = 0    # ELECTION: maior ID visto até agora
    leader_id: int = 0    # LEADER: ID do eleito
    payload: str = ""     # BCAST: operação a executar
    resource: str = ""    # REQUEST/GRANT: nome do recurso disputado

    def encode(self) -> bytes:
        return (json.dumps(asdict(self)) + "\n").encode("utf-8")

    @classmethod
    def decode(cls, raw: str) -> "Packet":
        return cls(**json.loads(raw))
