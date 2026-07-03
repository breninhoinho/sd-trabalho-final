"""
MODULO 4 - Checkpoint local e recuperacao.
[Van Steen Cap.8 - Tolerancia a Falhas: checkpointing e recuperacao por rollback]

Grava o estado em disco (volume Docker) de forma ATOMICA (write-tmp + rename),
para que um 'docker kill' no meio de uma escrita nao corrompa o historico.
"""
import os
import json
import logging

log = logging.getLogger("checkpoint")


class Checkpoint:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def save(self, data):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)   # rename atomico: nunca deixa arquivo pela metade

    def load(self):
        if not os.path.exists(self.path):
            return None
        try:
            with open(self.path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning("Checkpoint corrompido (%s), ignorando.", e)
            return None
