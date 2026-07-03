# SD-Trafego — Sistema Distribuído de Telemetria e Controle de Tráfego

Protótipo de sistema distribuído **descentralizado**, baseado em eventos (**Publish-Subscribe** com **Apache Kafka**), resiliente a falhas, partições de rede, assincronismo e deriva de relógios. Totalmente conteinerizado com **Docker Compose**.

---

## 1. Resumo: como funciona

São **8 contêineres** numa rede Docker isolada (`sdnet`):

| Papel | Contêineres | O que faz |
|---|---|---|
| **Broker** | `kafka` | Middleware Pub-Sub central (modo KRaft, sem Zookeeper) |
| **Sensores** (Mód. 2) | `sensor-1/2/3` | Publicam fluxo de veículos com **relógio de Lamport** |
| **Semáforos** (Mód. 3) | `semaforo-1/2/3/4` | Atuadores; **eleição de líder (Bully)** com **quórum** |

Tópicos Kafka usados: `traffic-data` (telemetria), `control` (heartbeats + eleição), `clock-sync` (Berkeley).

**Fluxo:** os sensores publicam dados o tempo todo. Os semáforos consomem, **reordenam por Lamport** e só o **líder** (eleito por maioria) controla os semáforos. Heartbeats detectam quedas; checkpoints permitem recuperação; o líder sincroniza os relógios físicos via Berkeley.

### Os 4 módulos

- **Módulo 1 — Infra e Caos:** `docker-compose.yml` + scripts `tc/netem` e `iptables` em `modulo1_infra/chaos/`.
- **Módulo 2 — Produtores e Tempo Lógico:** `modulo2_sensores/` — sensores + `LamportClock`.
- **Módulo 3 — Consumidores e Liderança:** `modulo3_semaforos/` — semáforos + `BullyElection` (com quórum) + buffer de reordenação.
- **Módulo 4 — Estado e Tolerância a Falhas:** `modulo4_estado/` — `Checkpoint` (recuperação) + `Berkeley` (sync de relógios).

---

## 2. Como rodar

Pré-requisitos: **Docker** e **Docker Compose**.

```bash
cd sd-trafego

# subir tudo (a primeira vez compila a imagem; aguarde o Kafka ficar "healthy")
docker compose up --build -d

# ver os logs do sistema
docker compose logs -f

# logs de um nó específico
docker compose logs -f semaforo-1

# derrubar tudo (e apagar os checkpoints)
docker compose down -v
```

Em ~30s você verá nos logs: sensores publicando (`PUBLICADO L=...`), semáforos recebendo/reordenando, e um líder eleito (`>>> EU SOU O LIDER`).

---

## 3. Demonstrações ao vivo (os 3 cenários de caos)

> Os scripts ficam em `modulo1_infra/chaos/`. Rode-os do **host** (eles usam `docker exec`).

### Restrição A — Inversão temporal (ordenação causal)
```bash
./modulo1_infra/chaos/latency.sh          # injeta 0–4000ms de latência + 5% de perda
docker compose logs -f semaforo-1
```
**Prova:** compare as linhas `RECEBIDO (chegada física) L=...` (ordem em que a rede entregou) com `PROCESSADO (ordem causal) L=...` (ordem crescente de Lamport). Mensagens atrasadas são reordenadas antes de processar.

### Restrição B — Partição de rede / split-brain (quórum)
```bash
# opção 1: matar o líder -> com 3 nós ainda há quórum -> novo líder é eleito
docker kill semaforo-<id_do_lider_atual>

# opção 2: partição 2x2 -> nenhum lado tem maioria (precisa de 3 de 4)
./modulo1_infra/chaos/partition.sh
docker compose logs -f semaforo-1 semaforo-3
```
**Prova:** no kill, os logs mostram `INICIANDO ELEICAO` e `EU SOU O LIDER`. Na partição 2x2, **ambos** os lados entram em `MODO DE SEGURANCA` (sem líder) — split-brain evitado. (Para mostrar um lado sobrevivendo, isole só 1 nó: `./modulo1_infra/chaos/partition.sh semaforo-4`.)

### Restrição C — Deriva de relógio físico (Berkeley)
Cada sensor sobe com um `CLOCK_OFFSET` diferente (ex.: +5s, −3s, +8.5s). O líder roda o Berkeley periodicamente:
```bash
docker compose logs -f | grep BERKELEY
```
**Prova:** linhas `BERKELEY round N: media=...` e `relógio ajustado em +X.XXXs` — os relógios convergem sem NTP externo.

### Recuperação (Módulo 4)
```bash
docker kill semaforo-2        # queda abrupta; o Docker reinicia (restart: unless-stopped)
docker compose logs -f semaforo-2
```
**Prova:** ao reiniciar, aparece `RECUPERADO checkpoint: lamport=..., processadas=...` — o nó retoma o estado anterior sem corromper o histórico.

### Limpar o caos
```bash
./modulo1_infra/chaos/heal.sh   # remove latência, perda e partição; quórum volta
```

---

## 4. Mapeamento teórico (livro Van Steen)

| Capítulo | Conceito | Onde no código |
|---|---|---|
| Cap. 2 | Arquitetura baseada em eventos (Pub-Sub) | `docker-compose.yml`, `common/config.py` (tópicos) |
| Cap. 3 | Processos / conteinerização | `Dockerfile`, serviços do compose |
| Cap. 4 | Comunicação por mensagens (middleware) | `common/kafka_io.py` |
| Cap. 6 | Relógios lógicos e ordenação causal | `modulo2_sensores/lamport.py`, `modulo3_semaforos/reorder.py` |
| Cap. 6 | Eleição de líder (Bully) + sync de relógios (Berkeley) | `modulo3_semaforos/election.py`, `modulo4_estado/berkeley.py` |
| Cap. 7/8 | Consistência, quórum, tolerância a falhas e recuperação | `election.py` (quórum), `modulo4_estado/state.py` (checkpoint) |

---

## 5. Tratamento de exceções de rede
`common/kafka_io.py` trata `NoBrokersAvailable`/`ConnectionRefused` (broker subindo, com retry) e `KafkaTimeoutError` (rede particionada) sem derrubar os processos — `safe_send` apenas registra a falha e segue.
