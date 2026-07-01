"""
Interface da shell distribuída.

Comandos:
  ls, cat, mkdir, write, lock, release, crash, exit
"""

import re
import sys
import threading


class Terminal:
    def __init__(self, hub):
        self._hub       = hub
        self._hub.shell_ref = self

        # VFS: sincronizado via multicast entre todos os nós
        self._files: dict[str, str] = {}
        self._dirs:  set[str]       = set()
        self._vfs_lock = threading.Lock()

    # ── Prompt ────────────────────────────────────────────────────────────────

    def _prompt(self) -> str:
        is_leader = self._hub.node_id == self._hub.leader_id
        tag = "[lider]" if is_leader else ""
        return f"node{self._hub.node_id}{tag}$ "

    def refresh_prompt(self):
        """Chamado pelos módulos de protocolo para sinalizar mudança de papel."""
        pass  # O prompt é reconstruído a cada iteração do loop

    # ── Loop principal ────────────────────────────────────────────────────────

    def run(self):
        print(f"\nNó {self._hub.node_id} ativo. Digite 'help' para listar os comandos.\n")
        while True:
            try:
                line = input(self._prompt()).strip()
            except (EOFError, KeyboardInterrupt):
                print("\nSaindo...")
                sys.exit(0)
            if not line:
                continue
            self._dispatch(line)

    # ── Dispatcher ────────────────────────────────────────────────────────────

    def _dispatch(self, line: str):
        tokens = line.split()
        cmd    = tokens[0].lower()

        if cmd == "help":
            self._do_help()
        elif cmd == "ls":
            self._do_ls()
        elif cmd == "cat":
            self._do_cat(tokens[1] if len(tokens) > 1 else None)
        elif cmd == "mkdir":
            self._do_mkdir(tokens[1] if len(tokens) > 1 else None)
        elif cmd == "write":
            self._do_write(line)
        elif cmd == "lock":
            self._do_lock(tokens[1] if len(tokens) > 1 else None)
        elif cmd == "release":
            self._do_release()
        elif cmd == "crash":
            self._hub.crash()
        elif cmd in ("exit", "quit"):
            sys.exit(0)
        else:
            print(f"Comando não reconhecido: '{cmd}'. Use 'help'.")

    # ── Comandos ──────────────────────────────────────────────────────────────

    def _do_help(self):
        print("""
  ls                       Lista o sistema de arquivos virtual
  cat <arquivo>            Exibe o conteúdo de um arquivo
  mkdir <dir>              Cria um diretório  (requer lock antes)
  write "txt" > arquivo    Cria/sobrescreve arquivo (requer lock antes)
  lock <recurso>           Solicita acesso exclusivo ao recurso
  release                  Libera o acesso e notifica os demais nós
  crash                    Simula a falha deste nó
  exit                     Encerra o processo
""")

    def _do_ls(self):
        with self._vfs_lock:
            dirs  = sorted(self._dirs)
            files = sorted(self._files.keys())
        if not dirs and not files:
            print("  (vazio)")
            return
        for d in dirs:
            print(f"  {d}/")
        for f in files:
            print(f"  {f}  ({len(self._files[f])} bytes)")

    def _do_cat(self, name: str | None):
        if not name:
            print("Uso: cat <arquivo>")
            return
        with self._vfs_lock:
            content = self._files.get(name)
        if content is None:
            print(f"  '{name}' não encontrado.")
        else:
            print(content)

    def _do_mkdir(self, name: str | None):
        if not name:
            print("Uso: mkdir <diretório>")
            return
        if not self._hub.mutex.has_permission:
            print("[!] Acesso negado. Use 'lock <recurso>' antes de criar diretórios.")
            return
        self._hub.bcast_mgr.send(f"mkdir:{name}")

    def _do_write(self, line: str):
        m = re.match(r'^write\s+"([^"]+)"\s*>\s*(\S+)$', line)
        if not m:
            m = re.match(r'^write\s+(.+?)\s*>\s*(\S+)$', line)
        if not m:
            print('Uso: write "conteudo" > arquivo.txt')
            return
        content, filename = m.group(1), m.group(2)
        if not self._hub.mutex.has_permission:
            print("[!] Acesso negado. Use 'lock <recurso>' antes de escrever.")
            return
        self._hub.bcast_mgr.send(f"write:{filename}:{content}")

    def _do_lock(self, resource: str | None):
        if not resource:
            print("Uso: lock <recurso>")
            return
        print(f"[RA] Solicitando acesso ao recurso '{resource}'...")
        if self._hub.mutex.lock(resource):
            print(f"[RA] Acesso concedido para '{resource}'.")
        else:
            print(f"[RA] Falha ao obter acesso para '{resource}'.")

    def _do_release(self):
        self._hub.mutex.unlock()
        print("[RA] Recurso liberado.")

    # ── Callback do multicast ─────────────────────────────────────────────────

    def apply_cmd(self, payload: str, origin: str):
        """
        Aplica uma operação recebida via multicast no VFS local.

        Formatos de payload suportados:
          mkdir:<nome_do_diretório>
          write:<nome_arquivo>:<conteudo>
        """
        with self._vfs_lock:
            if payload.startswith("mkdir:"):
                dirname = payload[6:]
                self._dirs.add(dirname)
                print(f"\n[VFS] Diretório '{dirname}' criado — {origin}")

            elif payload.startswith("write:"):
                rest = payload[6:]
                sep  = rest.find(":")
                if sep == -1:
                    print(f"\n[VFS] Payload malformado: '{payload}'")
                    return
                filename = rest[:sep]
                content  = rest[sep + 1:]
                self._files[filename] = content
                print(f"\n[VFS] '{filename}' atualizado — {origin}")

            else:
                print(f"\n[VFS] Operação desconhecida: '{payload}'")
