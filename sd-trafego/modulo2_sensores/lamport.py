"""
Relogio Logico de Lamport.
[Van Steen Cap.6 - Coordenacao: relogios logicos e ordenacao causal de eventos]

Regras:
  - Antes de ENVIAR um evento:  L = L + 1   (tick)
  - Ao RECEBER um evento:       L = max(L, L_recebido) + 1   (update)
Isso garante: se A causou B, entao timestamp(A) < timestamp(B).
"""
import threading


class LamportClock:
    def __init__(self, value=0):
        self.value = value
        self._lock = threading.Lock()

    def tick(self):
        """Evento local / antes de enviar."""
        with self._lock:
            self.value += 1
            return self.value

    def update(self, received):
        """Ao receber uma mensagem com timestamp 'received'."""
        with self._lock:
            self.value = max(self.value, received) + 1
            return self.value

    def read(self):
        with self._lock:
            return self.value

    def set(self, value):
        with self._lock:
            self.value = value
