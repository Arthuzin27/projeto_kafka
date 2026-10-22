# Demo Kafka: Pix em tempo real com detecção de fraude

Projeto de demonstração do Apache Kafka para seminário introdutório.
Um gerador publica transações Pix fictícias; dois serviços independentes
(antifraude e monitor) consomem essas transações ao mesmo tempo.

```
                                 ┌─────────────── KAFKA (Docker) ───────────────┐
                                 │                                              │
 gerador_pix.py  ──produz──►     │  tópico "transacoes"                         │
 (producer)       chave =        │   ├─ partição 0  [0][1][2][3]...             │
                  conta_origem   │   ├─ partição 1  [0][1][2]...                │
                                 │   └─ partição 2  [0][1][2][3][4]...          │
                                 │                                              │
                                 │  tópico "alertas" (3 partições)              │
                                 └───────┬───────────────────────┬──────────────┘
                                         │                       │
                          grupo "antifraude"              grupo "monitor"
                          antifraude.py (1 ou 2 inst.)    monitor.py
                          └─ publica suspeitas em "alertas"
```

**Onde o Kafka entra:** ele é o "meio de campo". O gerador não conhece o
antifraude nem o monitor: só escreve no tópico. Os consumidores não conhecem o
gerador: só leem do tópico. O Kafka guarda as mensagens em disco (log), divide
o tópico em partições e controla, para cada grupo, até onde ele já leu (offset).
Os scripts Python são apenas **clientes**; o Kafka em si roda no Docker.

| Conceito | Onde aparece na demo |
|---|---|
| **Tópico** | `transacoes` e `alertas` |
| **Partição** | `transacoes` tem 3; o gerador mostra em qual cada Pix caiu |
| **Chave** | `conta_origem`: a mesma conta sempre cai na mesma partição (ordem garantida) |
| **Consumer group** | `antifraude` e `monitor` leem o mesmo tópico sem atrapalhar um ao outro |
| **Rebalance** | subir/derrubar uma 2ª instância do antifraude redistribui as partições |
| **Offset** | o monitor desligado volta e continua exatamente de onde parou |
| **Retenção** | `monitor.py --do-inicio` relê tudo desde a primeira mensagem |

## Arquivos

| Arquivo | Função |
|---|---|
| `docker-compose.yml` | Kafka 4.1 (KRaft, 1 broker), criação dos tópicos e Kafka UI |
| `gerador_pix.py` | Producer: gera Pix e injeta suspeitas (valor alto e rajadas) |
| `antifraude.py` | Consumer group `antifraude`: regras de fraude, publica em `alertas` |
| `monitor.py` | Consumer group `monitor`: mostra cada Pix com partição, offset e totais |
| `requirements.txt` | Biblioteca Python `confluent-kafka` |

Os tópicos são criados automaticamente pelo container `kafka-init` quando você
roda `docker compose up -d`. Não é preciso nenhum comando extra.

---

## 1. Pré-requisitos

- **Docker Desktop** (Windows/Mac) ou **Docker Engine + Compose** (Linux)
- **Python 3.9 a 3.13**
- ~2 GB de RAM livres

### Windows
1. Instale o Docker Desktop: https://www.docker.com/products/docker-desktop/ (aceite usar WSL 2 se ele pedir) e abra-o.
2. Instale o Python em https://www.python.org/downloads/ marcando **"Add python.exe to PATH"**.
3. No PowerShell, dentro da pasta do projeto:
   ```powershell
   python -m venv .venv
   .venv\Scripts\python -m pip install -r requirements.txt
   ```

### Linux / Mac
```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

> Neste README os comandos usam `.venv\Scripts\python` (Windows). No Linux/Mac
> troque por `.venv/bin/python`. Se preferir, ative o ambiente virtual
> (`.venv\Scripts\Activate.ps1` ou `source .venv/bin/activate`) e use só `python`.

## 2. Subir o ambiente e conferir

```bash
docker compose up -d
```

Na primeira vez ele baixa as imagens (~1 GB). Depois, confira:

```bash
docker compose ps
```
`kafka` e `kafka-ui` devem estar `Up` (o `kafka` com `healthy`). O `kafka-init`
aparece como encerrado (`Exited (0)`); isso é normal, ele só cria os tópicos.

```bash
docker logs kafka-init
```
Deve listar `transacoes` e `alertas` com `PartitionCount: 3`.

Abra **http://localhost:8080**: o cluster `pix-local` deve aparecer com os dois tópicos.

---

## 3. ROTEIRO DA DEMO (4–5 min)

**Antes de apresentar:** ambiente no ar (`docker compose up -d`), navegador
aberto no Kafka UI e **4 terminais** abertos na pasta do projeto, lado a lado:

| Terminal | Vai rodar |
|---|---|
| T1 | gerador |
| T2 | monitor |
| T3 | antifraude (instância 1) |
| T4 | antifraude (instância 2) |

Dica: rode a demo inteira uma vez antes da aula e depois **resete** (seção 5),
para começar com o tópico vazio.

### a) Mostrar o tópico com 3 partições
No Kafka UI: **Topics → transacoes**. Mostre a aba *Overview* com as partições 0, 1 e 2.

> 🗣️ "Um **tópico** é como uma fila nomeada de eventos. Este está dividido em
> 3 **partições**: é assim que o Kafka espalha a carga e permite que vários
> consumidores trabalhem em paralelo. Cada partição é um log onde as mensagens
> só são acrescentadas no final, numeradas pelo **offset**."

### b) Ligar o gerador e ver as mensagens chegando
```bash
# T1
.venv\Scripts\python gerador_pix.py
# T2
.venv\Scripts\python monitor.py
```
No T1 aparece cada Pix com `[partição X | offset Y]`. No Kafka UI, abra
**transacoes → Messages** e mostre as mensagens chegando.

> 🗣️ "O gerador é o **producer**. Ele usa a conta de origem como **chave**: o
> Kafka faz um hash da chave, então a conta `ana` sempre cai na mesma partição.
> Isso garante a **ordem** das transações de uma mesma conta. O monitor é um
> **consumer** do grupo `monitor`: ele mostra a partição e o offset de cada
> mensagem que lê."

### c) Ligar o antifraude e ver os alertas
```bash
# T3
.venv\Scripts\python antifraude.py
```
Aparece em verde `REBALANCE! ... partições: [0, 1, 2]` (sozinho, ele pega tudo)
e, de vez em quando, `🚨 ALERTA` em vermelho. No Kafka UI: **Topics → alertas → Messages**.

> 🗣️ "O antifraude é outro **consumer group**. Ele lê **o mesmo tópico** que o
> monitor, mas cada grupo tem seus próprios offsets: um não rouba mensagem do
> outro. O gerador nem sabe que esses serviços existem: isso é
> **desacoplamento**. O antifraude também é **producer**: publica no tópico
> `alertas`, que outro sistema (um app de notificação, por exemplo) poderia consumir."

### d) Segunda instância do antifraude → rebalance
```bash
# T4
.venv\Scripts\python antifraude.py
```
Em poucos segundos o T3 mostra `Devolvendo partições [0, 1, 2]` e depois
`REBALANCE! ... [0, 1]`; o T4 mostra `REBALANCE! ... [2]`.
No Kafka UI: **Consumers → antifraude** mostra os 2 membros e quem lê cada partição.

> 🗣️ "Subi uma segunda cópia do mesmo serviço, no **mesmo grupo**. O Kafka fez
> um **rebalance**: dividiu as partições entre os dois. Dentro de um grupo,
> **cada partição é lida por só uma instância**: é assim que se escala
> horizontalmente. Por isso o número de partições é o limite de paralelismo:
> uma 4ª instância ficaria parada."
>
> (Se perguntarem: a regra "mais de 3 Pix em 10 s" continua funcionando com 2
> instâncias porque, graças à chave, todas as transações de uma conta vão para a
> mesma partição e, portanto, para a mesma instância.)

### e) Derrubar uma instância → a outra assume
No **T4** aperte **Ctrl+C**. O T3 mostra `REBALANCE! ... [0, 1, 2]`.

> 🗣️ "Uma instância caiu e a outra assumiu as partições dela automaticamente,
> continuando do último offset salvo. Nenhuma transação ficou sem análise:
> isso é **tolerância a falhas**."

> ⚠️ Use **Ctrl+C**. Se você fechar a janela do terminal, o Kafka só percebe a
> queda depois de ~10 s (`session.timeout.ms`). Isso também é uma boa explicação
> ("o broker espera o *heartbeat* do consumidor"), mas demora mais.

### f) Parar o monitor por 30 s e ver a recuperação
No **T2** aperte **Ctrl+C**. Enquanto espera ~30 s, mostre no Kafka UI
**Consumers → monitor**: o **Lag** (mensagens pendentes) vai subindo.
Depois religue:
```bash
# T2
.venv\Scripts\python monitor.py
```
Ele despeja de uma vez todas as transações perdidas, marcadas com
`⏮ recuperada (Xs atrás)`, e depois volta ao ritmo normal. O lag volta a 0.

> 🗣️ "Enquanto o monitor estava fora, as mensagens não se perderam: ficaram
> guardadas no Kafka. O grupo `monitor` tinha salvo seu **offset**, então, ao
> voltar, continuou exatamente de onde parou. Diferente de uma fila
> tradicional, ler não apaga a mensagem: ela fica no log pelo período de
> **retenção** (aqui, 7 dias)."

### Extra (se sobrar tempo): reprocessar tudo desde o início
```bash
# T2 (Ctrl+C antes)
.venv\Scripts\python monitor.py --do-inicio
```
> 🗣️ "Como o Kafka **retém** o log, posso reler o histórico inteiro, por exemplo
> para recalcular um relatório ou testar uma regra nova de fraude com dados antigos."

Para encerrar: **Ctrl+C** em todos os terminais.

---

## 4. Problemas comuns

| Sintoma | Causa provável | Solução |
|---|---|---|
| `error during connect` / `Cannot connect to the Docker daemon` | Docker parado | Abra o Docker Desktop e espere ficar "running" (Linux: `sudo systemctl start docker`) |
| `Bind for 0.0.0.0:8080 failed: port is already allocated` | Outro programa usa a 8080 (Airflow, Tomcat, Jenkins...) | Suba o UI em outra porta. PowerShell: `$env:KAFKA_UI_PORTA=8081; docker compose up -d`. Linux/Mac: `KAFKA_UI_PORTA=8081 docker compose up -d`. Acesse `http://localhost:8081` |
| `port is already allocated` na **9092** | Outro Kafka rodando | `docker ps` para achar e parar o container (`docker stop <nome>`) |
| Scripts mostram `Connection refused` / `localhost:9092 ... failed` | Kafka ainda iniciando ou parado | `docker compose ps` e espere o `kafka` ficar `healthy` (~20 s) |
| `UNKNOWN_TOPIC_OR_PART` | Tópicos não foram criados | `docker compose up -d kafka-init` e confira `docker logs kafka-init` |
| Consumer liga mas não mostra nada por ~10 s | Uma instância anterior foi fechada à força e o grupo espera ela expirar | Aguarde ~10 s; da próxima vez pare com Ctrl+C |
| Antifraude não mostra alertas | Normal nos primeiros segundos | As suspeitas são sorteadas; espere ~30 s ou aumente `CHANCE_VALOR_ALTO` no `gerador_pix.py` |
| `ModuleNotFoundError: confluent_kafka` | Rodou com o Python errado | Use `.venv\Scripts\python` (ou ative o venv) e rode `pip install -r requirements.txt` |
| Cores aparecem como `[92m` | Terminal antigo | Use o **Windows Terminal** ou o terminal do VS Code |
| Kafka UI abre mas sem cluster | UI subiu antes do broker | `docker compose restart kafka-ui` |

## 5. Resetar tudo para ensaiar de novo

Pare os scripts (Ctrl+C) e rode:

```bash
docker compose down
docker compose up -d
```

`down` remove os containers, e com eles todas as mensagens, offsets e grupos.
O `up` cria tudo de novo do zero, com os tópicos vazios.

Para resetar só o offset do monitor, sem apagar as mensagens:
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group monitor --reset-offsets --to-earliest --topic transacoes --execute
```
(o monitor precisa estar desligado. No Windows, rode no PowerShell: o Git Bash
transforma `/opt/...` em um caminho do Windows e o comando falha)

## 6. Ajustes

Todas as configurações ficam no topo de cada script:

- `gerador_pix.py`: `INTERVALO_MIN/MAX`, `CHANCE_VALOR_ALTO`, `CHANCE_RAJADA`, `CONTAS`
- `antifraude.py`: `LIMITE_VALOR` (R$ 5.000), `LIMITE_TRANSACOES` (3), `JANELA_SEGUNDOS` (10)
- `monitor.py`: `ATRASO_DESTAQUE` (a partir de quantos segundos uma mensagem é marcada como "recuperada")
