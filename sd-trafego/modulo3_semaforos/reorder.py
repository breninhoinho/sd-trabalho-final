"""
MODULO 3 / Restricao A - Buffer de reordenacao causal.
[Van Steen Cap.6 - Ordenacao de eventos por relogios logicos]

A rede entrega mensagens FORA DE ORDEM (latencia de ate 4s). Em vez de processar
na ordem de CHEGADA, guardamos as mensagens num heap ordenado pelo timestamp de
Lamport e so as entregamos depois de uma janela (HOLD_BACK > maior latencia).
Assim qualquer mensagem atrasada ja chegou e tudo e processado em ordem LOGICA.
"""
import time
import heapq


class ReorderBuffer:
    def __init__(self, hold_back):
        self.hold = hold_back
        self.heap = []          # (lamport, sensor_id, arrival_time, msg)
        self._seq = 0

    def push(self, lamport, sensor_id, msg):
        self._seq += 1
        # desempate por sensor_id para ordem total deterministica
        heapq.heappush(self.heap, (lamport, sensor_id, time.time(), self._seq, msg))

    def ready(self):
        """Retorna, em ordem de Lamport, as mensagens que ja passaram da janela de espera."""
        out = []
        agora = time.time()
        while self.heap and (agora - self.heap[0][2]) >= self.hold:
            lamport, sensor_id, _, _, msg = heapq.heappop(self.heap)
            out.append((lamport, sensor_id, msg))
        return out
