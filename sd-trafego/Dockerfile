# Imagem unica usada por sensores e semaforos.
# Inclui iproute2 (tc/netem) e iptables para os scripts de Caos do Modulo 1.
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        iproute2 iptables procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# O comando concreto (sensor ou semaforo) e definido no docker-compose.
