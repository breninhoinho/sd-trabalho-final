"""
MODULO 3 - No Atuador (Semaforo Inteligente) - processo principal.
Junta os 4 modulos no comportamento do no:
  - Consome dados de trafego e os REORDENA por Lamport      (Mod.2/3, Restricao A)
  - Participa da ELEICAO Bully com quorum                    (Mod.3, Restricao B)
  - Emite/recebe HEARTBEATS e detecta falhas                (Mod.4)
  - Salva CHECKPOINT e se recupera apos docker kill         (Mod.4, Restricao C: clock)
  - Quando e LIDER, roda o coordenador de Berkeley          (Mod.4, Restricao C)

[Van Steen Cap.2,3,4,6,7,8]
"""
import os
import time
import logging
import threading

from common import config
from common.kafka_io import make_producer, make_consumer, safe_send
from modulo2_sensores.lamport import LamportClock
from modulo3_semaforos.reorder import ReorderBuffer
from modulo3_semaforos.election import BullyElection
from modulo4_estado.state import Checkpoint
from modulo4_estado.berkeley import SyncedClock, run_berkeley_coordinator

NODE_ID = int(os.getenv("NODE_ID", "1"))
NAME = f"semaforo-{NODE_ID}"

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{NAME}] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(NAME)


class Semaforo:
    def __init__(self):
        self.lamport = LamportClock()
        self.clock = SyncedClock()
        self.buffer = ReorderBuffer(config.HOLD_BACK)
        self.checkpoint = Checkpoint(os.path.join(config.DATA_DIR, f"{NAME}.json"))
        self.processed = 0
        self.estado_luz = "VERMELHO"
        self.running = True
        self.lock = threading.Lock()

        # membros vivos: node_id -> (last_seen, role)
        self.members = {NODE_ID: (time.time(), "semaforo")}
        # respostas de Berkeley coletadas quando somos lider
        self.clock_replies = {}

        self._recuperar()

        self.producer = make_producer(config.KAFKA_BOOTSTRAP)
        self.consumer = make_consumer(
            config.KAFKA_BOOTSTRAP,
            [config.TOPIC_CONTROL, config.TOPIC_TRAFFIC, config.TOPIC_CLOCK],
            group_id=f"{NAME}-grp",
        )

        self.eleicao = BullyElection(
            node_id=NODE_ID,
            send_fn=self._send_control,
            alive_fn=self._semaforos_vivos,
            quorum_fn=config.quorum_necessario,
            election_timeout=config.ELECTION_TIMEOUT,
        )
        self._ultimo_berkeley = 0
        self._berkeley_round = 0

    # ---------- Modulo 4: recuperacao ----------
    def _recuperar(self):
        estado = self.checkpoint.load()
        if estado:
            self.lamport.set(estado.get("lamport", 0))
            self.clock.adjustment = estado.get("adjustment", 0.0)
            self.processed = estado.get("processed", 0)
            self.estado_luz = estado.get("estado_luz", "VERMELHO")
            log.info("RECUPERADO checkpoint: lamport=%d, processadas=%d, luz=%s",
                     self.lamport.read(), self.processed, self.estado_luz)
        else:
            log.info("Iniciando sem checkpoint (no novo).")

    def _salvar(self):
        with self.lock:
            self.checkpoint.save({
                "lamport": self.lamport.read(),
                "adjustment": self.clock.adjustment,
                "processed": self.processed,
                "estado_luz": self.estado_luz,
            })

    # ---------- helpers ----------
    def _send_control(self, msg):
        msg.setdefault("node_id", NODE_ID)
        safe_send(self.producer, config.TOPIC_CONTROL, msg)

    def _semaforos_vivos(self):
        agora = time.time()
        vivos = set()
        with self.lock:
            for nid, (last, role) in self.members.items():
                if role == "semaforo" and (agora - last) <= config.MEMBER_TIMEOUT:
                    vivos.add(nid)
        vivos.add(NODE_ID)
        return vivos

    # ---------- loop de consumo (thread) ----------
    def _consume_loop(self):
        while self.running:
            try:
                batch = self.consumer.poll(timeout_ms=500)
                for tp, records in batch.items():
                    for rec in records:
                        self._dispatch(tp.topic, rec.value)
            except Exception as e:
                log.warning("Erro de rede no consumo: %s", e)
                time.sleep(1)

    def _dispatch(self, topic, msg):
        if topic == config.TOPIC_TRAFFIC:
            self._on_traffic(msg)
        elif topic == config.TOPIC_CONTROL:
            self._on_control(msg)
        elif topic == config.TOPIC_CLOCK:
            self._on_clock(msg)

    # ---------- Restricao A: recebe trafego e atualiza Lamport ----------
    def _on_traffic(self, msg):
        ts = msg.get("lamport", 0)
        self.lamport.update(ts)   # regra de recebimento de Lamport
        self.buffer.push(ts, msg.get("sensor_id", "?"), msg)
        log.info("RECEBIDO (chegada fisica) L=%d de %s [%s, %d veic] -> bufferizado",
                 ts, msg.get("sensor_id"), msg.get("road"), msg.get("vehicles"))

    # ---------- Modulo 4: heartbeats + Modulo 3: controle/eleicao ----------
    def _on_control(self, msg):
        t = msg.get("type")
        origem = msg.get("node_id")
        if t == "HB":
            role = msg.get("role", "semaforo")
            with self.lock:
                self.members[origem] = (time.time(), role)
        else:
            # mensagens de eleicao Bully
            if isinstance(origem, int):
                self.eleicao.on_message(msg)

    # ---------- Restricao C: Berkeley ----------
    def _on_clock(self, msg):
        t = msg.get("type")
        if t == "TIME_REQUEST":
            safe_send(self.producer, config.TOPIC_CLOCK, {
                "type": "TIME_REPLY", "node_id": NAME,
                "time": self.clock.synced(), "round": msg.get("round"),
            })
        elif t == "TIME_REPLY":
            if self.eleicao.leader == NODE_ID:   # so o lider coleta
                self.clock_replies[msg["node_id"]] = msg["time"]
        elif t == "TIME_ADJUST":
            ajuste = msg.get("adjustments", {}).get(NAME)
            if ajuste is not None:
                self.clock.apply(ajuste)
                log.info("BERKELEY: relogio ajustado em %+.3fs", ajuste)

    # ---------- entrega causal (chamada no loop principal) ----------
    def _entregar_reordenadas(self):
        for lamport, sensor_id, msg in self.buffer.ready():
            with self.lock:
                self.processed += 1
            sou_lider = self.eleicao.leader == NODE_ID
            # so o LIDER de fato controla o semaforo (coordenador unico)
            if sou_lider and not self.eleicao.safe_mode:
                nova = "VERDE" if msg.get("vehicles", 0) > 40 else "VERMELHO"
                with self.lock:
                    self.estado_luz = nova
                log.info("PROCESSADO (ordem causal) L=%d de %s -> LIDER define luz=%s",
                         lamport, sensor_id, nova)
            else:
                log.info("PROCESSADO (ordem causal) L=%d de %s -> (seguidor, sem atuar)",
                         lamport, sensor_id)

    # ---------- loop principal ----------
    def run(self):
        threading.Thread(target=self._consume_loop, daemon=True).start()
        log.info("Semaforo online. Quorum necessario=%d de %d.",
                 config.quorum_necessario(), config.TOTAL_SEMAFOROS)
        last_hb = 0
        last_ckpt = 0
        while self.running:
            agora = time.time()

            # 1) heartbeat (Modulo 4 - deteccao de falhas)
            if agora - last_hb >= config.HEARTBEAT_INTERVAL:
                self._send_control({"type": "HB", "node_id": NODE_ID, "role": "semaforo"})
                last_hb = agora

            # 2) eleicao / quorum (Modulo 3 - Restricao B)
            self.eleicao.tick()

            # 3) entrega causal das mensagens (Restricao A)
            self._entregar_reordenadas()

            # 4) Berkeley periodico se eu for lider (Restricao C)
            if (self.eleicao.leader == NODE_ID and not self.eleicao.safe_mode
                    and agora - self._ultimo_berkeley >= config.BERKELEY_INTERVAL):
                self._berkeley_round += 1
                self._ultimo_berkeley = agora
                threading.Thread(
                    target=run_berkeley_coordinator,
                    args=(self.producer, lambda: self.clock_replies,
                          self.clock_replies.clear, self.clock, self._berkeley_round),
                    daemon=True,
                ).start()

            # 5) checkpoint (Modulo 4)
            if agora - last_ckpt >= config.CHECKPOINT_INTERVAL:
                self._salvar()
                last_ckpt = agora

            time.sleep(1.0)


if __name__ == "__main__":
    Semaforo().run()
