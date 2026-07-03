#!/usr/bin/env bash
# MODULO 1 - Caos: Restricao B (particao de rede / split-brain)
# Isola os nos indicados do broker Kafka (bloqueia a porta 9092 com iptables).
# Default: isola semaforo-3 e semaforo-4 -> particao 2x2.
# Como o quorum exige 3 de 4, NENHUM lado tera maioria => ambos entram em
# MODO DE SEGURANCA (nenhum lider). Isso PROVA a prevencao de split-brain.
#
# Para mostrar um lado sobrevivendo (3x1), isole so um no:  ./partition.sh semaforo-4
set -u
NODES="${*:-semaforo-3 semaforo-4}"
for n in $NODES; do
  echo "[caos] particionando $n (bloqueando acesso ao broker)"
  docker exec "$n" iptables -A OUTPUT -p tcp --dport 9092 -j DROP
  docker exec "$n" iptables -A INPUT  -p tcp --sport 9092 -j DROP
done
echo "Particao ativa. Observe o modo de seguranca / quorum nos logs:"
echo "  docker compose logs -f semaforo-1 semaforo-3"
