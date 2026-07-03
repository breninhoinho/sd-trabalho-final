"""
MODULO 4 / Restricao C - Sincronizacao de relogios FISICOS (Algoritmo de Berkeley).
[Van Steen Cap.6 - Coordenacao: sincronizacao de relogios fisicos]

- SyncedClock: relogio fisico de cada no, COM deriva artificial (offset + skew) e
  um 'adjustment' calculado pela sincronizacao. Nao usa NTP externo (proibido).
- run_berkeley_coordinator: roda no LIDER. Pergunta o horario de todos via Pub-Sub,
  calcula a MEDIA e devolve a cada no o offset para convergirem ao mesmo tempo.
"""
import time
import logging

from common import config
from common.kafka_io import safe_send

log = logging.getLogger("berkeley")


class SyncedClock:
    """Relogio fisico derivado + ajuste de sincronizacao."""
    def __init__(self, offset=0.0, skew=0.0):
        self.base_offset = offset      # erro fixo (segundos)
        self.skew = skew               # erro proporcional ao tempo decorrido
        self.t0 = time.time()
        self.adjustment = 0.0          # correcao calculada pelo Berkeley

    def physical(self):
        """Tempo do relogio fisico LOCAL, ja com a deriva artificial aplicada."""
        real = time.time()
        return real + self.base_offset + self.skew * (real - self.t0)

    def synced(self):
        """Tempo apos aplicar o ajuste de sincronizacao distribuida."""
        return self.physical() + self.adjustment

    def apply(self, offset):
        self.adjustment += offset


def run_berkeley_coordinator(producer, get_replies, reset_replies, my_clock, rnd):
    """
    Executado pelo lider. Fluxo do Algoritmo de Berkeley:
      1. Broadcast TIME_REQUEST.
      2. Coleta TIME_REPLY de todos (durante uma janela).
      3. Calcula a media dos horarios.
      4. Envia a cada no o offset = media - horario_do_no.
    'get_replies' retorna o dict {node_id: time} coletado pelo loop de consumo.
    """
    reset_replies()
    safe_send(producer, config.TOPIC_CLOCK, {"type": "TIME_REQUEST", "round": rnd})
    log.info("BERKELEY round %d: TIME_REQUEST enviado.", rnd)

    time.sleep(3.0)  # janela de coleta de respostas

    replies = dict(get_replies())
    replies["__lider__"] = my_clock.synced()   # o lider tambem participa
    media = sum(replies.values()) / len(replies)

    ajustes = {nid: (media - t) for nid, t in replies.items() if nid != "__lider__"}
    # aplica o ajuste do proprio lider localmente
    my_clock.apply(media - my_clock.synced())

    safe_send(producer, config.TOPIC_CLOCK, {
        "type": "TIME_ADJUST", "adjustments": ajustes, "round": rnd,
    })
    log.info("BERKELEY round %d: media=%.3f, %d nos sincronizados.", rnd, media, len(ajustes))
