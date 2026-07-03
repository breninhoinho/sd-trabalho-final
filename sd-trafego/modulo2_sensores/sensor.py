"""
MODULO 2 - Produtor (Publisher) / No Sensor de Trafego.
[Van Steen Cap.2 Processos | Cap.4 Comunicacao | Cap.6 Relogios Logicos | Cap.7 Recuperacao]

O que faz:
  - Publica dados de fluxo de veiculos no topico 'traffic-data'.
  - Anexa um Relogio Logico de Lamport em CADA mensagem (ordenacao causal - Restricao A).
  - Tem um relogio FISICO com deriva artificial (Restricao C) e participa do
    algoritmo de Berkeley para se sincronizar.
  - Salva checkpoint local e se recupera apos 'docker kill' (Modulo 4).
"""
import os
import time
import random
import logging
import threading

from common import config
from common.kafka_io import make_producer, make_consumer, safe_send
from modulo2_sensores.lamport import LamportClock
from modulo4_estado.berkeley import SyncedClock
from modulo4_estado.state import Checkpoint

SENSOR_ID = os.getenv("SENSOR_ID", "sensor-x")
# Deriva artificial do relogio fisico (Restricao C): offset inicial e taxa de erro
CLOCK_OFFSET = float(os.getenv("CLOCK_OFFSET", "0"))   # segundos a mais/menos
CLOCK_SKEW = float(os.getenv("CLOCK_SKEW", "0"))       # erro proporcional ao tempo

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{SENSOR_ID}] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(SENSOR_ID)

ROADS = ["Av. Brasil", "Rua 7", "Av. Central", "Rua das Flores"]


class Sensor:
    def __init__(self):
        self.lamport = LamportClock()
        self.clock = SyncedClock(offset=CLOCK_OFFSET, skew=CLOCK_SKEW)
        self.checkpoint = Checkpoint(os.path.join(config.DATA_DIR, f"{SENSOR_ID}.json"))
        self.sent = 0
        self.running = True
        self._recuperar()
        self.producer = make_producer(config.KAFKA_BOOTSTRAP)
        self.consumer = make_consumer(
            config.KAFKA_BOOTSTRAP, [config.TOPIC_CLOCK], group_id=f"{SENSOR_ID}-clock"
        )

    # ---------- Modulo 4: recuperacao transparente ----------
    def _recuperar(self):
        estado = self.checkpoint.load()
        if estado:
            self.lamport.set(estado.get("lamport", 0))
            self.clock.adjustment = estado.get("adjustment", 0.0)
            self.sent = estado.get("sent", 0)
            log.info("RECUPERADO checkpoint: lamport=%d, enviadas=%d, ajuste=%.3fs",
                     self.lamport.read(), self.sent, self.clock.adjustment)
        else:
            log.info("Iniciando sem checkpoint (no novo).")

    def _salvar(self):
        self.checkpoint.save({
            "lamport": self.lamport.read(),
            "adjustment": self.clock.adjustment,
            "sent": self.sent,
        })

    # ---------- Modulo 4 / Restricao C: Berkeley (lado cliente) ----------
    def _clock_loop(self):
        while self.running:
            try:
                batch = self.consumer.poll(timeout_ms=500)
                for _, records in batch.items():
                    for rec in records:
                        self._on_clock_msg(rec.value)
            except Exception as e:
                log.warning("Erro no consumo de clock-sync: %s", e)
                time.sleep(1)

    def _on_clock_msg(self, msg):
        t = msg.get("type")
        if t == "TIME_REQUEST":
            # Coordenador (lider) pediu nosso horario -> respondemos com o relogio fisico derivado
            safe_send(self.producer, config.TOPIC_CLOCK, {
                "type": "TIME_REPLY",
                "node_id": SENSOR_ID,
                "time": self.clock.synced(),
                "round": msg.get("round"),
            })
        elif t == "TIME_ADJUST":
            ajuste = msg.get("adjustments", {}).get(SENSOR_ID)
            if ajuste is not None:
                self.clock.apply(ajuste)
                log.info("BERKELEY: relogio ajustado em %+.3fs -> agora=%.3f",
                         ajuste, self.clock.synced())

    # ---------- Modulo 2: publicacao de telemetria ----------
    def run(self):
        threading.Thread(target=self._clock_loop, daemon=True).start()
        log.info("Sensor online. Publicando em '%s'.", config.TOPIC_TRAFFIC)
        last_ckpt = 0
        while self.running:
            ts = self.lamport.tick()                       # Lamport: evento de envio
            msg = {
                "sensor_id": SENSOR_ID,
                "lamport": ts,                              # relogio LOGICO (ordenacao causal)
                "physical_ts": round(self.clock.synced(), 3),  # relogio FISICO sincronizado
                "road": random.choice(ROADS),
                "vehicles": random.randint(0, 80),
            }
            if safe_send(self.producer, config.TOPIC_TRAFFIC, msg, key=SENSOR_ID):
                self.sent += 1
                log.info("PUBLICADO L=%d | %s | %d veiculos", ts, msg["road"], msg["vehicles"])
            # tambem emite heartbeat para deteccao de falhas (Modulo 4)
            safe_send(self.producer, config.TOPIC_CONTROL, {
                "type": "HB", "node_id": SENSOR_ID, "role": "sensor", "ts": time.time(),
            })

            now = time.time()
            if now - last_ckpt >= config.CHECKPOINT_INTERVAL:
                self._salvar()
                last_ckpt = now

            time.sleep(random.uniform(1.0, 3.0))


if __name__ == "__main__":
    Sensor().run()
