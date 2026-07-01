# Autor: Leonardo Cenedes Pereira
"""
Shell distribuída com exclusão mútua por Ricart-Agrawala,
eleição de líder em anel e multicast ordenado por Relógio de Lamport.

Uso:
  python main.py --id <ID>

Exemplo (abra 3 terminais separados):
  python main.py --id 1
  python main.py --id 2
  python main.py --id 3

As configurações de cada nó (endereço e porta) são lidas de config.json.
O nó de maior ID inicia como líder da sessão.
"""

import argparse
import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

from hub        import Hub
from terminal   import Terminal
from mutex_ra   import RicartAgrawala
from leader     import RingLeader
from broadcaster import LamportBroadcast


def load_config(node_id: int) -> tuple[dict, list[dict]]:
    """Carrega config.json e separa o nó local dos demais."""
    path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    cluster = data["cluster"]
    me = next((n for n in cluster if n["id"] == node_id), None)
    if me is None:
        print(f"Erro: ID {node_id} não encontrado em config.json.")
        sys.exit(1)

    peers = [n for n in cluster if n["id"] != node_id]
    return me, peers


def main():
    parser = argparse.ArgumentParser(description="Shell Distribuída")
    parser.add_argument("--id", type=int, required=True, help="ID único deste nó")
    args = parser.parse_args()

    me, peers = load_config(args.id)

    print(f"Iniciando nó {args.id}  ({me['address']}:{me['port']})")
    print(f"Peers conhecidos: {[p['id'] for p in peers]}\n")

    hub = Hub(
        node_id = me["id"],
        address = me["address"],
        port    = me["port"],
        peers   = peers,
    )

    terminal = Terminal(hub)

    hub.mutex        = RicartAgrawala(hub)
    hub.election_mgr = RingLeader(hub)
    hub.bcast_mgr    = LamportBroadcast(hub, apply_fn=terminal.apply_cmd)

    hub.start()
    hub.election_mgr.start_probe()

    terminal.run()


if __name__ == "__main__":
    main()
