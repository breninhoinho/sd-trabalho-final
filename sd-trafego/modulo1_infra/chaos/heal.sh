#!/usr/bin/env bash
# MODULO 1 - Cura: remove TODO o caos (latencia, perda e particao).
# Uso: ./heal.sh
set -u
NODES="sensor-1 sensor-2 sensor-3 semaforo-1 semaforo-2 semaforo-3 semaforo-4"
for n in $NODES; do
  echo "[cura] limpando $n"
  docker exec "$n" tc qdisc del dev eth0 root 2>/dev/null && echo "   netem removido" || true
  docker exec "$n" iptables -F 2>/dev/null && echo "   iptables limpo" || true
done
echo "Rede normalizada. O quorum deve voltar e um lider sera eleito novamente."
