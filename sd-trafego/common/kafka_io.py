"""
Helpers de comunicacao com o broker Kafka (Pub-Sub).
[Van Steen Cap.4 - Comunicacao: middleware orientado a mensagens / message-oriented]

Trata as excecoes de rede exigidas pelo criterio de avaliacao:
  - NoBrokersAvailable / ConnectionRefused (broker ainda subindo)
  - KafkaTimeoutError (rede particionada ou lenta)
"""
import json
import time
import logging

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

log = logging.getLogger("kafka_io")


def make_producer(bootstrap, retries=60):
    """Cria um produtor, esperando o broker ficar disponivel (tolerancia a falha de inicializacao)."""
    for tentativa in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=1,
                retries=3,
                linger_ms=10,
                request_timeout_ms=5000,
                max_block_ms=2000,
            )
        except NoBrokersAvailable:
            log.warning("Broker indisponivel (tentativa %d). Aguardando...", tentativa + 1)
            time.sleep(2)
    raise RuntimeError("Nao foi possivel conectar ao broker Kafka")


def make_consumer(bootstrap, topics, group_id, retries=60):
    """Cria um consumidor. group_id unico por no => todos recebem TODAS as mensagens (broadcast)."""
    for tentativa in range(retries):
        try:
            return KafkaConsumer(
                *topics,
                bootstrap_servers=bootstrap,
                group_id=group_id,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
        except NoBrokersAvailable:
            log.warning("Broker indisponivel para consumer (tentativa %d). Aguardando...", tentativa + 1)
            time.sleep(2)
    raise RuntimeError("Nao foi possivel conectar ao broker Kafka")


def safe_send(producer, topic, value, key=None):
    """Envia tratando falhas de rede sem derrubar o processo."""
    try:
        producer.send(topic, value=value, key=key)
        return True
    except KafkaError as e:
        log.warning("Falha de rede ao publicar em %s: %s", topic, e)
        return False
    except Exception as e:  # KafkaTimeoutError quando particionado
        log.warning("Timeout/erro de rede ao publicar em %s: %s", topic, e)
        return False
