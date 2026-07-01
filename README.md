# Shell Distribuída

Este projeto é uma implementação de uma shell interativa que funciona de forma distribuída. A ideia principal é demonstrar na prática o comportamento de sistemas distribuídos, com múltiplos nós rodando simultaneamente e compartilhando um Sistema de Arquivos Virtual (VFS).

Para resolver os problemas clássicos de comunicação em rede e concorrência, o sistema implementa:

- **Eleição de Líder (Algoritmo em Anel)**: Garante que a rede sempre tenha um nó coordenador. Caso o líder atual caia ou perca conexão, uma nova eleição é feita automaticamente de forma descentralizada.
- **Exclusão Mútua (Ricart-Agrawala)**: Evita que dois nós alterem o mesmo dado ao mesmo tempo (condição de corrida). Um nó precisa solicitar e receber permissão dos demais antes de modificar arquivos ou diretórios.
- **Multicast Ordenado (Relógio de Lamport)**: Sincroniza a ordem dos eventos e mensagens entre todos os participantes da rede, garantindo que o estado do sistema de arquivos seja consistente em todos os terminais.

## Como rodar o projeto

O arquivo `config.json` já está preparado para um cluster local com 3 nós. Para simular a rede, você precisa rodar cada nó em um processo/terminal isolado.

Abra 3 terminais separados e inicie os nós com seus respectivos IDs:

Terminal 1:
```bash
python main.py --id 1
```

Terminal 2:
```bash
python main.py --id 2
```

Terminal 3:
```bash
python main.py --id 3
```

O sistema identifica o nó de maior ID e o define como líder inicial automaticamente. A partir daí, você já pode começar a enviar comandos.

## Comandos da Shell

Com a aplicação rodando, você pode usar os comandos abaixo para interagir com o VFS e testar os algoritmos distribuídos:

- `ls`: Lista os diretórios e arquivos presentes no VFS.
- `cat <arquivo>`: Lê o conteúdo de um arquivo.
- `lock <recurso>`: Solicita permissão exclusiva da rede para modificar um recurso. É obrigatório executar antes de tentar alterar o sistema de arquivos.
- `mkdir <dir>`: Cria um novo diretório.
- `write "texto" > arquivo`: Escreve ou sobrescreve o conteúdo de um arquivo.
- `release`: Libera o lock de um recurso para que outros nós possam acessá-lo.
- `crash`: Derruba o nó atual para simular uma falha (útil para forçar a eleição de um novo líder).
- `exit` ou `quit`: Encerra o terminal de forma limpa.
- `help`: Lista a descrição dos comandos disponíveis.

## Autor

Desenvolvido por Leonardo Cenedes Pereira.
