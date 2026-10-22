"""
CONSUMER GROUP "monitor": lê "transacoes" e mostra cada uma com
partição e offset, além de um contador de volume.

Ele lê o MESMO tópico que o antifraude, mas em outro grupo: cada grupo
tem seus próprios offsets e recebe todas as mensagens.

Uso:
  python monitor.py              -> continua de onde parou (offset salvo)
  python monitor.py --do-inicio  -> reprocessa o tópico inteiro (retenção)
Parar: Ctrl+C
"""
import json
import os
import sys
from datetime import datetime

from confluent_kafka import OFFSET_BEGINNING, Consumer

# ======================== CONFIGURAÇÕES ========================
BROKER = "localhost:9092"
TOPICO = "transacoes"
GRUPO = "monitor"
ATRASO_DESTAQUE = 3   # segundos: mensagens mais velhas que isso são "recuperadas"
# ===============================================================

# Deixa cores e emoji funcionarem no terminal do Windows
os.system("")
sys.stdout.reconfigure(encoding="utf-8")
CINZA, AZUL, NEGRITO, RESET = "\033[90m", "\033[94m", "\033[1m", "\033[0m"

do_inicio = "--do-inicio" in sys.argv

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GRUPO,
    "auto.offset.reset": "earliest",  # grupo novo começa do início do tópico
    "session.timeout.ms": 10_000,     # se a instância sumir sem avisar, o broker espera 10 s
})


def ao_receber_particoes(consumer, particoes):
    """Se pedido, volta cada partição para o offset 0 (início do log)."""
    if do_inicio:
        for p in particoes:
            p.offset = OFFSET_BEGINNING
        consumer.assign(particoes)
        print(f"{AZUL}⏪ Voltando ao início das partições {[p.partition for p in particoes]}{RESET}")
    else:
        print(f"{AZUL}▶️  Continuando do último offset salvo nas partições "
              f"{[p.partition for p in particoes]}{RESET}")


consumer.subscribe([TOPICO], on_assign=ao_receber_particoes)
print(f"📊 Monitor (grupo '{GRUPO}') conectado em {BROKER}  (Ctrl+C para parar)\n")

total_msgs = 0
total_valor = 0.0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erro: {msg.error()}")
            continue

        t = json.loads(msg.value())
        total_msgs += 1
        total_valor += t["valor"]

        # Mensagem antiga = estava esperando no Kafka enquanto o monitor estava desligado
        atraso = (datetime.now() - datetime.fromisoformat(t["timestamp"])).total_seconds()
        marca = f"{CINZA}⏮ recuperada ({atraso:.0f}s atrás){RESET}" if atraso > ATRASO_DESTAQUE else ""

        print(f"[partição {msg.partition()} | offset {msg.offset():>4}] "
              f"{t['conta_origem']:>8} → {t['conta_destino']:<8} R$ {t['valor']:>10,.2f}  "
              f"{NEGRITO}#{total_msgs}  total R$ {total_valor:,.2f}{RESET} {marca}")
except KeyboardInterrupt:
    print("\n🛑 Encerrando monitor...")
finally:
    consumer.close()  # salva os offsets: na volta ele continua daqui
    print(f"Monitor encerrado. Leu {total_msgs} transações nesta execução.")
