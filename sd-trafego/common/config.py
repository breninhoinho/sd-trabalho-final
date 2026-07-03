"""
Configuracoes globais compartilhadas por todos os modulos.
[Van Steen Cap.2 - Arquiteturas / modelo Publish-Subscribe baseado em eventos]
"""
import os

# Broker Kafka (Modulo 1 - Infraestrutura)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Topicos Pub-Sub
TOPIC_TRAFFIC = "traffic-data"   # sensores -> semaforos (dados de fluxo de veiculos)
TOPIC_CONTROL = "control"        # semaforos <-> semaforos (heartbeat, eleicao Bully)
TOPIC_CLOCK   = "clock-sync"     # sincronizacao de relogios (Berkeley)

# Quantidade total de semaforos no sistema (usado para calcular QUORUM)
TOTAL_SEMAFOROS = int(os.getenv("TOTAL_SEMAFOROS", "4"))

# Quorum = maioria estrita (mais de 50% dos nos totais)
# [Restricao B - Prevencao de Split-Brain]
def quorum_necessario():
    return (TOTAL_SEMAFOROS // 2) + 1

# Tempos (segundos)
HEARTBEAT_INTERVAL = 2.0    # de quanto em quanto tempo cada no anuncia que esta vivo
MEMBER_TIMEOUT     = 6.0    # sem heartbeat por esse tempo = no considerado morto
ELECTION_TIMEOUT   = 4.0    # espera por respostas na eleicao Bully
HOLD_BACK          = 5.0    # janela de reordenacao causal (> maior latencia da rede 4s)
CHECKPOINT_INTERVAL = 5.0   # de quanto em quanto tempo salva o checkpoint local
BERKELEY_INTERVAL  = 12.0   # de quanto em quanto tempo o lider sincroniza os relogios

# Diretorio de checkpoints locais (volume Docker) [Modulo 4]
DATA_DIR = os.getenv("DATA_DIR", "/data")
