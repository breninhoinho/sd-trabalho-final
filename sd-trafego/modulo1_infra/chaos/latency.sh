#!/usr/bin/env bash
# MODULO 1 - Caos: Restricao A (rede nao-confiavel)
# Injeta latencia flutuante (~0 a 4000ms) e 5% de perda de pacotes via tc/netem.
# Uso: ./latency.sh                 (aplica em todos os nos)
#      ./latency.sh sensor-1 ...    (apenas nos listados)
set -u
NODES="${*:-sensor-1 sensor-2 sensor-3 semaforo-1 semaforo-2 semaforo-3 semaforo-4}"
for n in $NODES; do
  echo "[caos] netem em $n: delay 2000ms +-2000ms, loss 5%"
  docker exec "$n" tc qdisc replace dev eth0 root netem \
      delay 2000ms 2000ms distribution normal loss 5% \
      && echo "   ok" || echo "   FALHOU (no offline?)"
done
echo "Latencia/perda aplicadas. Veja os logs reordenando por Lamport:"
echo "  docker compose logs -f semaforo-1"
