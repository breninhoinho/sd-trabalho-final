# SD-Trafego — Documentação Técnica

Protótipo de sistema distribuído descentralizado de telemetria e controle de tráfego urbano. Baseado em eventos (Publish-Subscribe com Apache Kafka), resiliente a falhas, partições de rede, assincronia e deriva de relógios. Totalmente conteinerizado com Docker Compose.

**Integrantes:**
- Artur Kioshi de Almeida Nacafucasaco (802405)
- Breno Dias Arantes dos Santos (800577)

---

## Visão Geral da Arquitetura

O sistema é composto por **8 contêineres** em uma rede Docker isolada (`sdnet`):

| Contêiner | Papel |
|---|---|
| `kafka` | Broker Pub-Sub central (modo KRaft, sem Zookeeper) |
| `sensor-1`, `sensor-2`, `sensor-3` | Produtores: publicam fluxo de veículos com relógio de Lamport |
| `semaforo-1`, `semaforo-2`, `semaforo-3`, `semaforo-4` | Atuadores: eleição de líder (Bully) com quórum |

**Tópicos Kafka:**

| Tópico | Uso |
|---|---|
| `traffic-data` | Sensores → Semáforos (telemetria de veículos) |
| `control` | Semáforos ↔ Semáforos (heartbeats + eleição Bully) |
| `clock-sync` | Semáforo líder ↔ Todos (sincronização Berkeley) |

**Fluxo principal:**
1. Sensores publicam eventos de tráfego continuamente, com timestamp de Lamport.
2. Todos os semáforos recebem e bufferizam os eventos.
3. Após a janela de espera, processam em ordem de Lamport (causal), não de chegada.
4. Apenas o **líder eleito** (com quórum) atualiza o estado do semáforo.
5. O líder também periodicamente sincroniza os relógios físicos via Berkeley.

```
sensor-1 ─┐
sensor-2 ──┤── [traffic-data] ──► semaforo-1
sensor-3 ─┘                      semaforo-2  ←─── [control] ───► (eleição / HB)
                                  semaforo-3
                                  semaforo-4 (líder) ──► [clock-sync] ──► todos
```

---

## Mapeamento Teórico (livro Van Steen)

| Capítulo | Conceito | Onde no código |
|---|---|---|
| Cap. 2 | Arquitetura baseada em eventos (Pub-Sub) | `docker-compose.yml`, `common/config.py` |
| Cap. 3 | Processos e conteinerização | `Dockerfile`, serviços do compose |
| Cap. 4 | Comunicação por mensagens (middleware) | `common/kafka_io.py` |
| Cap. 6 | Relógios lógicos e ordenação causal | `modulo2_sensores/lamport.py`, `modulo3_semaforos/reorder.py` |
| Cap. 6 | Eleição de líder (Bully) + sync Berkeley | `modulo3_semaforos/election.py`, `modulo4_estado/berkeley.py` |
| Cap. 7/8 | Consistência, quórum, tolerância a falhas | `election.py` (quórum), `modulo4_estado/state.py` (checkpoint) |

---

## Módulo 1 — Infraestrutura e Injeção de Caos

**Arquivos:** `docker-compose.yml`, `Dockerfile`, `modulo1_infra/chaos/`

Este módulo não contém lógica de sistema distribuído — ele **cria o ambiente** onde os outros módulos precisam operar corretamente mesmo sob falhas. É composto por três partes:

### 1.1 Orquestração (`docker-compose.yml`)

Define todos os 8 contêineres e suas relações:

- **`depends_on: kafka: {condition: service_healthy}`** — nenhum nó sobe antes do Kafka responder. O healthcheck usa `kafka-topics.sh` a cada 10s com até 12 tentativas.
- **`cap_add: [NET_ADMIN]`** — permite que cada contêiner altere suas próprias regras de rede com `tc` e `iptables`. Sem isso, os scripts de caos não funcionam.
- **`restart: unless-stopped`** — reinicia automaticamente contêineres que caírem por falha.
- **`volumes: ["state:/data"]`** — volume compartilhado para persistência de checkpoints entre reinícios.
- **Deriva artificial de relógio** — cada sensor recebe variáveis distintas para simular relógios dessincronizados (Restrição C):
  - `sensor-1`: `CLOCK_OFFSET=+5.0s`, `CLOCK_SKEW=+0.002`
  - `sensor-2`: `CLOCK_OFFSET=-3.0s`, `CLOCK_SKEW=-0.001`
  - `sensor-3`: `CLOCK_OFFSET=+8.5s`, `CLOCK_SKEW=+0.003`

### 1.2 Imagem Docker (`Dockerfile`)

Usa `python:3.11-slim` como base. Instala `iproute2`, `iptables` e `procps` explicitamente — ferramentas necessárias para os scripts de caos funcionarem dentro dos contêineres. Uma única imagem é usada por todos os 7 contêineres Python; o comportamento de cada um é definido pelo campo `command` no compose.

### 1.3 Scripts de Caos (`modulo1_infra/chaos/`)

Rodados do **host** via `docker exec`, injetam problemas nos contêineres em execução:

**`latency.sh` — Restrição A (rede não confiável)**

```bash
./modulo1_infra/chaos/latency.sh
```

Usa `tc qdisc netem` para injetar `2000ms ±2000ms` de latência e 5% de perda de pacotes. Mensagens chegam com 0ms a 4s de atraso — propositalmente maior que o `HOLD_BACK` do buffer de reordenação — para forçar chegadas fora de ordem. Demonstra a necessidade do relógio de Lamport.

**`partition.sh` — Restrição B (partição de rede / split-brain)**

```bash
./modulo1_infra/chaos/partition.sh              # partição 2x2 (padrão)
./modulo1_infra/chaos/partition.sh semaforo-4   # partição 3x1
```

Usa `iptables` para bloquear a porta 9092 (Kafka) nos nós indicados. Por padrão isola `semaforo-3` e `semaforo-4` (2×2). Como o quórum exige 3 de 4, nenhum lado tem maioria e ambos entram em modo de segurança sem eleger líder — prevenindo split-brain.

**`heal.sh` — Restauração**

```bash
./modulo1_infra/chaos/heal.sh
```

Desfaz todo o caos: remove regras `tc` e limpa `iptables -F` em todos os nós. O quórum se restaura e uma nova eleição ocorre.

---

## Módulo 2 — Sensores e Tempo Lógico

**Arquivos:** `modulo2_sensores/sensor.py`, `modulo2_sensores/lamport.py`

Implementa os produtores do sistema. Cada sensor é um processo independente que publica eventos de tráfego continuamente no tópico `traffic-data`.

### 2.1 Relógio de Lamport (`lamport.py`)

Implementação thread-safe do Relógio Lógico de Lamport. Garante ordenação causal entre eventos distribuídos:

- **`tick()`** — chamado antes de enviar: `L = L + 1`
- **`update(received)`** — chamado ao receber: `L = max(L, L_recebido) + 1`

A propriedade garantida é: se o evento A causou B, então `timestamp(A) < timestamp(B)`, independente de qualquer diferença de relógio físico entre os nós.

### 2.2 Sensor (`sensor.py`)

Cada instância de sensor tem três responsabilidades:

**Publicação de telemetria:**
A cada 1–3 segundos (intervalo aleatório), publica uma mensagem no tópico `traffic-data` com:
- `lamport`: timestamp lógico (incrementado antes do envio)
- `physical_ts`: horário físico ajustado pela sincronização Berkeley
- `road`: via aleatória entre 4 opções
- `vehicles`: quantidade de veículos (0–80)

**Participação no Berkeley (lado cliente):**
O sensor escuta o tópico `clock-sync`. Ao receber `TIME_REQUEST` do líder, responde com seu horário físico atual (com deriva). Ao receber `TIME_ADJUST`, aplica o offset calculado pelo líder ao seu `SyncedClock`.

**Checkpoint e recuperação:**
A cada `CHECKPOINT_INTERVAL` (5s), salva `{lamport, adjustment, sent}` em disco. Ao reiniciar, carrega o checkpoint e retoma o estado anterior sem perder a contagem de Lamport.

---

## Módulo 3 — Semáforos, Liderança e Ordenação Causal

**Arquivos:** `modulo3_semaforos/semaforo.py`, `modulo3_semaforos/election.py`, `modulo3_semaforos/reorder.py`

Implementa os consumidores/atuadores do sistema. Cada semáforo executa um loop principal que coordena todos os aspectos de sistema distribuído.

### 3.1 Buffer de Reordenação Causal (`reorder.py`)

Implementa um heap (`heapq`) ordenado por timestamp de Lamport. Ao receber uma mensagem, ela é inserida no heap com seu tempo de chegada. A entrega ocorre apenas para mensagens que já esperaram pelo menos `HOLD_BACK` segundos (5s), janela propositalmente maior que a latência máxima injetada (4s).

Isso garante que qualquer mensagem atrasada pela rede já chegou antes de ser processada, e a entrega ocorre em **ordem causal**, não em ordem de chegada física.

**Exemplo:**
```
RECEBIDO (chegada fisica) L=15 de sensor-2   ← chegou primeiro
RECEBIDO (chegada fisica) L=13 de sensor-1   ← chegou atrasado
...5s depois...
PROCESSADO (ordem causal) L=13 de sensor-1   ← processado primeiro (Lamport menor)
PROCESSADO (ordem causal) L=15 de sensor-2
```

### 3.2 Eleição de Líder com Quórum (`election.py`)

Implementa o **algoritmo do Valentão (Bully)** com adaptação crítica de quórum para evitar split-brain.

**Bully clássico:**
- O nó de maior ID vivo vira coordenador.
- Ao iniciar eleição, envia `ELECTION` para todos os nós com ID maior.
- Se receber `ANSWER`, recua (um maior está vivo).
- Se não receber resposta no `ELECTION_TIMEOUT` (4s), se declara líder e envia `COORDINATOR` para todos.

**Adaptação de quórum (Restrição B):**
- Um nó só inicia eleição se enxergar **quórum** (`⌊N/2⌋ + 1 = 3` de 4 semáforos).
- Um nó só aceita ser líder se ainda houver quórum no momento de assumir.
- Um nó só reconhece um `COORDINATOR` recebido se tiver quórum.
- Sem quórum → **modo de segurança**: `leader = None`, nenhuma atuação ocorre.

**Detecção de falhas:**
Cada semáforo emite heartbeats (`HB`) a cada `HEARTBEAT_INTERVAL` (2s). Um nó é considerado morto após `MEMBER_TIMEOUT` (6s) sem heartbeat. O `tick()` do loop principal checa isso a cada iteração.

### 3.3 Loop Principal (`semaforo.py`)

O loop central de cada semáforo executa em sequência a cada 1 segundo:

1. **Heartbeat** — publica `HB` no tópico `control`
2. **Eleição** — chama `election.tick()` para detectar falhas e disparar/resolver eleições
3. **Entrega causal** — chama `buffer.ready()` e processa mensagens em ordem de Lamport; só o líder atualiza `estado_luz`
4. **Berkeley** — se for líder, dispara periodicamente o coordenador de sincronização
5. **Checkpoint** — salva estado a cada 5s

---

## Módulo 4 — Estado, Tolerância a Falhas e Sincronização de Relógios

**Arquivos:** `modulo4_estado/state.py`, `modulo4_estado/berkeley.py`

### 4.1 Checkpoint e Recuperação (`state.py`)

Implementa persistência de estado em disco com **escrita atômica**. O padrão usado é *write-tmp + rename*:

1. Escreve o novo estado em um arquivo `.tmp`
2. Chama `fsync()` para garantir que o dado chegou ao disco
3. Usa `os.replace()` (rename atômico do SO) para substituir o arquivo definitivo

Isso garante que um `docker kill` no meio de uma escrita nunca deixa um arquivo corrompido. Na reinicialização, o nó sempre encontra ou o estado anterior completo ou nenhum arquivo (primeiro boot).

O estado salvo por cada semáforo inclui: `{lamport, adjustment, processed, estado_luz}`.

### 4.2 Sincronização de Relógios Físicos (`berkeley.py`)

Implementa o **algoritmo de Berkeley** sem NTP externo.

**`SyncedClock`:**
Relógio físico de cada nó com deriva artificial controlada:
- `physical()` = `time.time() + base_offset + skew * tempo_decorrido`
- `synced()` = `physical() + adjustment` (após sincronização)

**`run_berkeley_coordinator` (executado apenas pelo líder):**
1. Publica `TIME_REQUEST` no tópico `clock-sync` (broadcast)
2. Aguarda 3s para coletar as respostas `TIME_REPLY` de todos os nós
3. Inclui o próprio horário do líder no cálculo
4. Calcula a **média** de todos os horários recebidos
5. Publica `TIME_ADJUST` com o offset individual para cada nó: `ajuste = média - horario_do_no`
6. Aplica seu próprio ajuste localmente

O resultado é que todos os nós convergem para um horário médio comum sem depender de serviço externo.

---

## `common/` — Compartilhado

### `config.py`
Centraliza todas as constantes do sistema:

| Constante | Valor | Descrição |
|---|---|---|
| `HEARTBEAT_INTERVAL` | 2s | Frequência dos heartbeats |
| `MEMBER_TIMEOUT` | 6s | Tempo sem HB para considerar nó morto |
| `ELECTION_TIMEOUT` | 4s | Espera por respostas na eleição Bully |
| `HOLD_BACK` | 5s | Janela de espera do buffer de reordenação |
| `CHECKPOINT_INTERVAL` | 5s | Frequência de salvamento de checkpoint |
| `BERKELEY_INTERVAL` | 12s | Frequência de sincronização Berkeley |

O quórum é calculado dinamicamente: `(TOTAL_SEMAFOROS // 2) + 1`.

### `kafka_io.py`
Encapsula toda a comunicação com o broker:

- **`make_producer`** — cria produtor com retry automático em caso de `NoBrokersAvailable` (broker ainda subindo)
- **`make_consumer`** — cria consumidor com `group_id` único por nó, fazendo todos receberem todas as mensagens (broadcast efetivo)
- **`safe_send`** — envia sem lançar exceção em caso de falha de rede; apenas registra o erro em log. Trata `KafkaError` e `KafkaTimeoutError`.

---

## Como Executar

```bash
cd sd-trafego

# Subir tudo (primeira vez compila a imagem)
docker compose up --build -d

# Acompanhar logs em tempo real
docker compose logs -f

# Logs de um nó específico
docker compose logs -f semaforo-1

# Derrubar tudo (apaga checkpoints)
docker compose down -v
```

Em ~30s aparecerão nos logs: sensores publicando (`PUBLICADO L=...`), semáforos bufferizando e processando, e um líder eleito (`>>> EU SOU O LIDER`).

---

## Demonstrações dos Cenários de Caos

### Restrição A — Inversão temporal

```bash
./modulo1_infra/chaos/latency.sh
docker compose logs -f semaforo-1
```

Observe que as linhas `RECEBIDO (chegada fisica) L=...` chegam fora de ordem, mas as linhas `PROCESSADO (ordem causal) L=...` estão sempre em ordem crescente de Lamport.

### Restrição B — Partição e split-brain

```bash
# Partição 2x2: nenhum lado tem quórum → modo de segurança em ambos
./modulo1_infra/chaos/partition.sh
docker compose logs -f semaforo-1 semaforo-3

# Partição 3x1: lado majoritário elege líder, lado minoritário entra em modo seguro
./modulo1_infra/chaos/partition.sh semaforo-4
```

### Restrição C — Deriva de relógio

```bash
docker compose logs -f | grep BERKELEY
```

Linhas `BERKELEY round N: media=...` e `relogio ajustado em +X.XXXs` confirmam a convergência dos relógios sem NTP.

### Recuperação após falha

```bash
docker kill semaforo-2
docker compose logs -f semaforo-2
```

Após reinício: `RECUPERADO checkpoint: lamport=..., processadas=..., luz=...`

### Restaurar a rede

```bash
./modulo1_infra/chaos/heal.sh
```
