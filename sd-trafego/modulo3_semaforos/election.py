"""
MODULO 3 - Eleicao de Lider (Algoritmo do Valentao / Bully) COM Quorum.
[Van Steen Cap.6 - Coordenacao: eleicao de lider | Cap.8 - acordo sob particao]

Bully classico: o no de MAIOR id vivo vira coordenador.
Adaptacao critica (Restricao B - Split-Brain):
  Um no SO aceita ser/reconhecer lider se enxergar QUORUM (maioria estrita dos
  semaforos). Sem quorum -> MODO DE SEGURANCA (nenhum lider), evitando dois
  lideres em particoes diferentes.

As mensagens de controle (ELECTION / ANSWER / COORDINATOR) trafegam pelo proprio
broker Pub-Sub no topico 'control'.
"""
import time
import logging

log = logging.getLogger("election")


class BullyElection:
    def __init__(self, node_id, send_fn, alive_fn, quorum_fn, election_timeout):
        self.node_id = node_id                  # inteiro: maior vence
        self.send = send_fn                     # send(msg_dict)
        self.alive_members = alive_fn           # retorna set de ids de semaforos vivos (inclui self)
        self.quorum = quorum_fn                 # retorna quorum necessario (int)
        self.timeout = election_timeout

        self.leader = None
        self.safe_mode = False
        self.em_eleicao = False
        self._inicio_eleicao = 0
        self._recebeu_answer = False

    # ---------- recepcao de mensagens de controle ----------
    def on_message(self, msg):
        t = msg.get("type")
        origem = msg.get("node_id")

        if t == "ELECTION":
            # alguem de id MENOR comecou eleicao -> respondo que estou vivo e assumo a disputa
            if origem < self.node_id:
                self.send({"type": "ANSWER", "node_id": self.node_id, "to": origem})
                if not self.em_eleicao:
                    self.iniciar_eleicao()
        elif t == "ANSWER":
            if msg.get("to") == self.node_id:
                # um no MAIOR esta vivo -> ele assume, eu recuo
                self._recebeu_answer = True
        elif t == "COORDINATOR":
            # so aceito novo lider se houver quorum (impede split-brain)
            if len(self.alive_members()) >= self.quorum():
                self.leader = origem
                self.em_eleicao = False
                self.safe_mode = False
                log.info("Novo LIDER reconhecido: semaforo-%s", origem)

    # ---------- logica de disparo ----------
    def iniciar_eleicao(self):
        vivos = self.alive_members()
        if len(vivos) < self.quorum():
            self.entrar_modo_seguranca("sem quorum para iniciar eleicao")
            return
        self.em_eleicao = True
        self._recebeu_answer = False
        self._inicio_eleicao = time.time()
        maiores = [n for n in vivos if n > self.node_id]
        log.info("INICIANDO ELEICAO (Bully). Vivos=%s", sorted(vivos))
        if not maiores:
            # sou o maior vivo -> viro coordenador
            self.virar_coordenador()
        else:
            for n in maiores:
                self.send({"type": "ELECTION", "node_id": self.node_id, "to": n})

    def virar_coordenador(self):
        if len(self.alive_members()) < self.quorum():
            self.entrar_modo_seguranca("quorum perdido antes de assumir")
            return
        self.leader = self.node_id
        self.em_eleicao = False
        self.safe_mode = False
        self.send({"type": "COORDINATOR", "node_id": self.node_id})
        log.info(">>> EU SOU O LIDER (semaforo-%s). Quorum OK.", self.node_id)

    def entrar_modo_seguranca(self, motivo):
        if not self.safe_mode or self.leader is not None:
            log.warning("MODO DE SEGURANCA ativado (%s). Sem lider para evitar split-brain.", motivo)
        self.safe_mode = True
        self.leader = None
        self.em_eleicao = False

    # ---------- chamado periodicamente pelo loop principal ----------
    def tick(self):
        vivos = self.alive_members()

        # 1) sem quorum -> modo de seguranca (Restricao B)
        if len(vivos) < self.quorum():
            self.entrar_modo_seguranca(f"vivos={len(vivos)} < quorum={self.quorum()}")
            return

        if self.safe_mode:
            log.info("Quorum restaurado (vivos=%s). Saindo do modo de seguranca.", sorted(vivos))
            self.safe_mode = False
            self.leader = None

        # 2) eleicao em andamento: trata timeouts
        if self.em_eleicao:
            if time.time() - self._inicio_eleicao >= self.timeout:
                if self._recebeu_answer:
                    # um maior respondeu mas nao anunciou COORDINATOR a tempo -> tenta de novo
                    self.iniciar_eleicao()
                else:
                    self.virar_coordenador()
            return

        # 3) lider ausente ou morto -> inicia eleicao
        if self.leader is None or self.leader not in vivos:
            self.iniciar_eleicao()
