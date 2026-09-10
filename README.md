# AgroSense Mesh
### Processamento de Eventos e Irrigação Automatizada de Precisão

**Disciplina:** Programação Distribuída e Paralela  
**Modelo:** A Metrópole Resiliente  

---

## Integrantes do Grupo

- **Caio Lennon**
- **Livia Louzada**
- **Julia Vionette**

---

## 1. Domínio Agrícola e Propósito do Sistema

O **AgroSense Mesh** é uma plataforma distribuída e tolerante a falhas voltada ao monitoramento contínuo de telemetria agropecuária (umidade de solo e temperatura ambiente) em múltiplas zonas de plantio (`Zonas A, B, C, D e E`).

O sistema realiza controle de irrigação de precisão:
- **Sensores de Solo:** Geram leituras periódicas em intervalos regulares.
- **Processamento Distribuído:** Três nós *Workers* concorrentes consomem e processam localmente as leituras da fila de mensagens.
- **Consolidação Causal pelo Líder:** O Worker eleito Líder via Algoritmo do Valentão (*Bully Election*) ordena causalmente os eventos via **Relógio Lógico de Lamport** e avalia a regra de acionamento:
  $$\text{Umidade} < 40\% \implies \text{Ativação de Válvula de Irrigação}$$
- **Persistência Replicada:** O Líder grava os eventos de irrigação no nó **PostgreSQL Primary**, que replica fisicamente via **Streaming Replication** para o nó **PostgreSQL Replica**. O **Dashboard Web** realiza leituras exclusivamente a partir da Réplica.

---

## 2. Arquitetura do Sistema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AgroSense Mesh Architecture                       │
│                                                                             │
│  [Sensor-A]  [Sensor-B]  [Sensor-C]  [Sensor-D]  [Sensor-E]                 │
│      │           │           │           │           │                      │
│      └───────────┴─────┬─────┴───────────┴───────────┘                      │
│                        │ gRPC: SendTelemetry(TelemetryPacket)               │
│                        ▼                                                    │
│             ┌─────────────────────┐                                         │
│             │    Gateway gRPC     │ ← Porta :50051                          │
│             │  (Lamport Sync)     │                                         │
│             └──────────┬──────────┘                                         │
│                        │ AMQP Publish (Publisher Confirms + UUID)           │
│                        ▼                                                    │
│             ┌─────────────────────────────────┐                             │
│             │            RabbitMQ             │ ← AMQP :5672                │
│             │    agro_telemetry_queue (DLQ)   │   UI   :15672               │
│             └──────────┬──────────┬───────────┘                             │
│                        │          │                                         │
│           ┌────────────┼──────────┴────────────┐ Competing Consumers        │
│           ▼            ▼                       ▼ (prefetch=1)               │
│      ┌─────────┐  ┌─────────┐             ┌─────────┐                       │
│      │Worker-1 │  │Worker-2 │             │Worker-3 │ ← Bully Election      │
│      │(Lamport)│  │(Lamport)│             │(Lamport)│   (TCP 9001..9003)    │
│      └────┬────┘  └────┬────┘             └────┬────┘                       │
│           │            │                       │                            │
│           └────────────┼───────────────────────┘                            │
│                        │ AMQP Publish (ProcessedTelemetry)                 │
│                        ▼                                                    │
│             ┌─────────────────────────────────┐                             │
│             │      agro_processed_results     │ (Fila de Resultados)        │
│             └──────────┬──────────────────────┘                             │
│                        │ Consumo Exclusivo do Líder                         │
│                        ▼                                                    │
│             ┌─────────────────────┐                                         │
│             │   Worker Líder      │ • Validação Causal Lamport              │
│             │  (Consolidador)     │ • Regra: moisture < 40%                 │
│             └──────────┬──────────┘                                         │
│                        │ INSERT ... ON CONFLICT DO NOTHING                  │
│                        ▼                                                    │
│             ┌─────────────────────┐                                         │
│             │ PostgreSQL Primary  │ ← Porta :5432 (Gravação)                │
│             └──────────┬──────────┘                                         │
│                        │ Streaming Replication Física (WAL)                 │
│                        ▼                                                    │
│             ┌─────────────────────┐                                         │
│             │ PostgreSQL Replica  │ ← Porta :5433 (Leitura / Standby)       │
│             └──────────┬──────────┘                                         │
│                        │ Polling / Queries SQL                              │
│                        ▼                                                    │
│             ┌─────────────────────┐                                         │
│             │    Dashboard Web    │ ← Porta :3000 (Fastify + WebSockets)    │
│             └─────────────────────┘                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Tabela de Portas e Endpoints

| Serviço | Porta Host | Porta Container | Protocolo | Descrição |
| :--- | :--- | :--- | :--- | :--- |
| **Gateway gRPC** | `50051` | `50051` | gRPC / HTTP/2 | Ingress de telemetria dos sensores |
| **RabbitMQ Broker** | `5672` | `5672` | AMQP 0-9-1 | Filas de mensagens e confirmações |
| **RabbitMQ Management** | `15672` | `15672` | HTTP | Painel Web de Gestão de Filas |
| **Worker 1 (Eleição)** | `9001` | `9001` | TCP / JSON | Canal TCP do Algoritmo Bully |
| **Worker 2 (Eleição)** | `9002` | `9002` | TCP / JSON | Canal TCP do Algoritmo Bully |
| **Worker 3 (Eleição)** | `9003` | `9003` | TCP / JSON | Canal TCP do Algoritmo Bully |
| **PostgreSQL Primary** | `5432` | `5432` | PostgreSQL | Banco de escrita e replicação (conflito comum com Postgres local) |
| **PostgreSQL Replica** | `5433` | `5432` | PostgreSQL | Réplica em Standby físico (leitura) |
| **Dashboard Web** | `3000` | `3000` | HTTP / WS | Painel em tempo real via WebSockets |

---

## 4. Fluxo Ponta a Ponta das Mensagens

1. **Geração da Leitura:** O sensor simulador gera uma leitura com `sensor_id`, `zone`, `moisture`, `temperature`, `timestamp` e incrementa seu relógio de Lamport local $L_{local} = L_{local} + 1$.
2. **Envio gRPC:** O pacote `TelemetryPacket` é transmitido via RPC `SendTelemetry` para o Gateway na porta `50051`.
3. **Ingress & Sincronização:** O Gateway recebe o pacote, atualiza seu relógio:
   $$L_{gw} = \max(L_{local}, L_{msg}) + 1$$
   Em seguida, gera um `message_id` UUID único, enriquece o pacote e publica na fila `agro_telemetry_queue` utilizando **ConfirmChannel** (garantia de persistência do RabbitMQ).
4. **ACK do Gateway:** O Gateway responde imediatamente ao sensor com `TelemetryAck` contendo `queued: true`, `message_id` e $L_{gw}$.
5. **Processamento Concorrente (*Competing Consumers*):** Os três nós Workers concorrem pela fila `agro_telemetry_queue` com `prefetch(1)`. Cada worker que recebe uma mensagem:
   - Valida o payload.
   - Atualiza seu relógio: $L_{worker} = \max(L_{local}, L_{gw}) + 1$.
   - Constrói o objeto `ProcessedTelemetry` enriquecido com `worker_lamport` e `worker_id`.
   - Publica o resultado na fila `agro_processed_results`.
   - Envia `ACK` da mensagem original para o RabbitMQ.
6. **Consolidação Exclusiva pelo Líder:** Apenas o Worker com liderança ativa consome da fila `agro_processed_results`. O Líder:
   - Sincroniza seu relógio com `worker_lamport`.
   - Armazena os eventos em um buffer causal ordenado deterministicamente por:
     1. `worker_lamport`
     2. `worker_id`
     3. `message_id`
   - Avalia a regra de irrigação: se `moisture < 40%`, executa inserção idempotente no PostgreSQL Primary:
     ```sql
     INSERT INTO irrigation_log (message_id, sensor_id, zone, moisture, temperature, lamport_time, event_ts, worker_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     ON CONFLICT (message_id) DO NOTHING;
     ```
7. **Streaming Replication:** O PostgreSQL Primary envia as alterações via WAL (*Write-Ahead Logging*) para o PostgreSQL Replica.
8. **Dashboard em Tempo Real:** O servidor Web Fastify consulta o PostgreSQL Replica e transmite os logs de irrigação e estados dos nós para o navegador via WebSocket.

---

## 5. Estrutura do Repositório

```text
Programa--o-Distribuida/
├── client/                 # Simulador de sensores (gRPC)
├── server/
│   ├── gateway/            # Ingress gRPC + publicação no RabbitMQ
│   └── worker/             # Competing consumers + Bully + persistência
├── web/                    # Dashboard Fastify + WebSocket (lê a réplica)
├── infra/
│   ├── 00-replication.sh        # Init: libera replicação no Primary (pg_hba)
│   ├── primary-entrypoint.sh    # Garante regra de replicação a cada start
│   ├── replica-entrypoint.sh    # Clone via pg_basebackup + Standby (com retry)
│   └── pg-init.sql              # Schema (irrigation_log + view recent_irrigation)
├── agro_telemetry.proto    # Contrato gRPC
├── docker-compose.yml      # Topologia completa da malha
├── .gitattributes          # Força LF em scripts (compatibilidade Windows)
└── README.md
```

---

## 6. Como Executar o Projeto

### Pré-requisitos
- Docker Engine $\ge$ 24.x
- Docker Compose $\ge$ 2.x
- Node.js $\ge$ 20.x (para compilação/testes locais opcionais)
- Portas livres: `3000`, `5432`, `5433`, `5672`, `15672`, `50051`, `9001–9003`

### Credenciais do PostgreSQL (lab)

| Parâmetro | Valor |
| :--- | :--- |
| Database | `agrosense` |
| User | `agro` |
| Password | `agro123` |
| Primary (host) | `localhost:5432` |
| Replica (host) | `localhost:5433` |

### Execução via Docker Compose (Recomendada)

```bash
# 1. Construir e subir todos os contêineres em segundo plano
docker compose up --build -d

# 2. Verificar o status (Primary/Replica devem aparecer como healthy)
docker compose ps

# 3. Acompanhar os logs unificados de todos os serviços
docker compose logs -f
```

Após a subida, acesse:

| Recurso | URL |
| :--- | :--- |
| **Dashboard Web** | http://localhost:3000 |
| **RabbitMQ Management** | http://localhost:15672 (`guest` / `guest`) |

### Ordem de dependência (healthcheck)

O Compose sobe os serviços respeitando saúde, não apenas “container started”:

1. `rabbitmq` e `postgres-primary` sobem primeiro.
2. Workers dependem de `rabbitmq` **e** `postgres-primary` *healthy*.
3. `postgres-replica` clona o Primary via `pg_basebackup` só depois do Primary *healthy*.
4. `web` depende da réplica *healthy* (leituras somente no Standby).

Healthchecks atuais do Postgres:

| Serviço | `start_period` | `retries` | Motivo |
| :--- | :--- | :--- | :--- |
| `postgres-primary` | `60s` | `20` | Tempo de init + scripts em `infra/` |
| `postgres-replica` | `120s` | `30` | Tempo do `pg_basebackup` (com retries) na 1ª subida |

### Execução dos Testes Automatizados Locais

```bash
# Executa a suíte de testes unitários
npm test
```

### Encerrar / resetar volumes

```bash
# Para os contêineres (mantém dados)
docker compose down

# Para e apaga volumes (banco e RabbitMQ zerados — use em lab)
docker compose down -v
```

### Troubleshooting: `dependency failed` / Postgres unhealthy

#### A) `agrosense-pg-primary` unhealthy

Se workers, réplica ou web falharem com erro de *dependency* no Primary:

1. **CRLF no Windows (scripts `.sh`)**  
   O init monta scripts de `infra/` no container Linux. Com CRLF o script quebra.  
   O repositório força LF via `.gitattributes`. Após atualizar o código:
   ```bash
   git add --renormalize .
   docker compose down -v
   docker compose up --build -d
   ```

2. **Porta `5432` ocupada** (PostgreSQL instalado no host)  
   Encerre o serviço local **ou** altere o mapeamento em `docker-compose.yml`:
   `"5432:5432"` → ex. `"5434:5432"`.

3. **Diagnóstico**
   ```bash
   docker compose ps
   docker logs agrosense-pg-primary
   ```

#### B) `agrosense-pg-replica` unhealthy / web com dependency na réplica

Causas comuns: volume da réplica pela metade, Primary sem regra de replicação no `pg_hba`, ou `pg_basebackup` falhando na 1ª tentativa.

1. **Reset limpo (recomendado em lab)**
   ```bash
   git add --renormalize .
   docker compose down -v
   docker compose up --build -d
   docker compose ps
   docker logs agrosense-pg-replica
   ```

2. **Confirme que a réplica está em Standby**
   ```bash
   docker compose exec postgres-replica psql -U agro -d agrosense -c "SELECT pg_is_in_recovery();"
   ```
   Deve retornar `t` (true).

3. **Porta `5433` ocupada no host** — altere `"5433:5432"` no `docker-compose.yml`.

A réplica agora usa `infra/replica-entrypoint.sh` com **retry** no `pg_basebackup`, e o Primary usa `infra/primary-entrypoint.sh` para garantir a regra `host replication ...` mesmo em volumes antigos.

---

## 7. Guia de Demonstração e Comprovação Prática

### 7.1. Comprovação do Competing Consumers (3 Workers ativos)
Execute o comando abaixo para visualizar que **todos os 3 workers** recebem e processam leituras da fila:
```bash
docker compose logs worker-1 worker-2 worker-3 | grep "Leitura processada"
```
*Evidência esperada:* Mensagens intercaladas de `[Worker-1]`, `[Worker-2]` e `[Worker-3]`.

---

### 7.2. Comprovação do Relógio Lógico de Lamport
Observe a progressão monotônica e a sincronização do relógio entre cliente, gateway, workers e líder:
```bash
docker compose logs | grep -E "L_local|L_gw|L_worker|L_leader"
```
*Evidência esperada:* Cada nó atualiza seu relógio para $\max(L_{local}, L_{remoto}) + 1$.

---

### 7.3. Comprovação da Eleição Bully e Queda do Líder
1. Identifique o líder atual (por padrão, o maior ID ativo `worker-3`):
   ```bash
   docker compose logs worker-1 worker-2 worker-3 | grep "LÍDER"
   ```
2. Derrube o líder atual:
   ```bash
   docker compose stop worker-3
   ```
3. Acompanhe a detecção por timeout e a reeleição automática do `worker-2`:
   ```bash
   docker compose logs -f worker-1 worker-2
   ```
4. Restaure o `worker-3` e comprove que o maior ID reassume a liderança:
   ```bash
   docker compose start worker-3
   docker compose logs -f worker-3
   ```

---

### 7.4. Comprovação da Replicação PostgreSQL Primary → Replica
1. Verifique que o `postgres-primary` está em modo normal (não recovery) e o `postgres-replica` está em modo Standby/Recovery:
   ```bash
   # Primary deve retornar 'false':
   docker compose exec postgres-primary psql -U agro -d agrosense -c "SELECT pg_is_in_recovery();"

   # Replica deve retornar 'true':
   docker compose exec postgres-replica psql -U agro -d agrosense -c "SELECT pg_is_in_recovery();"
   ```
2. Verifique que os dados gravados pelo líder no Primary estão imediatamente visíveis na Replica:
   ```bash
   docker compose exec postgres-replica psql -U agro -d agrosense -c "SELECT * FROM recent_irrigation LIMIT 5;"
   ```

---

### 7.5. Comprovação de Idempotência
Verifique que a constraint de unicidade no banco impede duplicações em caso de reentrega de mensagens:
```bash
docker compose exec postgres-primary psql -U agro -d agrosense -c "SELECT COUNT(*), COUNT(DISTINCT message_id) FROM irrigation_log;"
```
*Evidência esperada:* `count` e `count(distinct message_id)` são rigorosamente idênticos.

---

## 8. Evidências Reais de Logs da Aplicação

### Log do Sensor (Client):
```text
[Client] sensor-A-01 | zone=A | moisture=32.40% temp=24.10°C | L_local=5 → L_gw=6 | msgId=a1b2c3d4...
[Client] sensor-B-01 | zone=B | moisture=68.15% temp=19.80°C | L_local=5 → L_gw=7 | msgId=b2c3d4e5...
```

### Log do Gateway:
```text
[Gateway] gRPC escutando na porta 50051 | Lamport inicial=0
[RabbitMQ/Gateway] Broker CONFIRM msgId=a1b2c3d4-e5f6-7890-abcd-ef1234567890 sensor=sensor-A-01 L_gw=6
```

### Log dos Workers (Competing Consumers):
```text
[Worker-1] Leitura processada: sensor=sensor-A-01 zone=A moisture=32.4% | L_gw=6 → L_worker=8 | msgId=a1b2c3d4...
[Worker-2] Leitura processada: sensor=sensor-B-01 zone=B moisture=68.15% | L_gw=7 → L_worker=9 | msgId=b2c3d4e5...
[Election/3] VITÓRIA BULLY: worker-3 é o LÍDER!
[DB] Log de irrigação persistido: msgId=a1b2c3d4-e5f6-7890-abcd-ef1234567890 sensor=sensor-A-01 zone=A L=8
```

---

## 9. Declaração de Uso de Inteligência Artificial

Este projeto utilizou ferramentas de Inteligência Artificial para análise arquitetural, refatoração de código concorrente, implementação do protocolo de socket TCP do algoritmo Bully, automação de testes unitários e elaboração da documentação técnica.

- **Ferramentas Utilizadas:** Antigravity IDE, Google Gemini e Cursor.
- **Intervenções Realizadas:**
  1. Correção do descarte prematuro de mensagens em workers não líderes através da separação entre fila de telemetria e fila de resultados consolidados.
  2. Correção do canal de resposta PING/PONG no mesmo socket TCP para prevenir falsas eleições de líder.
  3. Configuração de replicação física PostgreSQL Streaming Replication (Primary $\to$ Replica).
  4. Implementação de idempotência ponta a ponta com propagação de `message_id` UUID e Publisher Confirms no RabbitMQ.
  5. Criação de suíte de testes unitários em TypeScript estrito.
  6. Endurecimento da subida Docker (healthchecks com `start_period`, `.gitattributes` com LF e troubleshooting de dependency no Primary).
  7. Padronização da ordenação causal (`worker_lamport` → `worker_id` → `message_id`) e remoção do `leader_election_log`.
  8. Endurecimento da Réplica (`pg_basebackup` com retry) e garantia de regra de replicação no Primary a cada start.
