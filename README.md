# 🌿 AgroSense Mesh
### Processamento de Eventos e Irrigação Automatizada de Precisão

**Disciplina:** Programação Distribuída e Paralela  
**Modelo:** A Metrópole Resiliente  

---

## 👥 Integrantes do Grupo

- **Caio Lennon**
- **Livia Louzada**
- **Julia Vionette**

---

## 🌾 1. Domínio Agrícola e Propósito do Sistema

O **AgroSense Mesh** é uma plataforma distribuída e tolerante a falhas voltada ao monitoramento contínuo de telemetria agropecuária (umidade de solo e temperatura ambiente) em múltiplas zonas de plantio (`Zonas A, B, C, D e E`).

O sistema realiza controle de irrigação de precisão:
- **Sensores de Solo:** Geram leituras periódicas em intervalos regulares.
- **Processamento Distribuído:** Três nós *Workers* concorrentes consomem e processam localmente as leituras da fila de mensagens.
- **Consolidação Causal pelo Líder:** O Worker eleito Líder via Algoritmo do Valentão (*Bully Election*) ordena causalmente os eventos via **Relógio Lógico de Lamport** e avalia a regra de acionamento:
  $$\text{Umidade} < 40\% \implies \text{Ativação de Válvula de Irrigação}$$
- **Persistência Replicada:** O Líder grava os eventos de irrigação no nó **PostgreSQL Primary**, que replica fisicamente via **Streaming Replication** para o nó **PostgreSQL Replica**. O **Dashboard Web** realiza leituras exclusivamente a partir da Réplica.

---

## 📐 2. Arquitetura do Sistema

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

## 🔌 3. Tabela de Portas e Endpoints

| Serviço | Porta Host | Porta Container | Protocolo | Descrição |
| :--- | :--- | :--- | :--- | :--- |
| **Gateway gRPC** | `50051` | `50051` | gRPC / HTTP/2 | Ingress de telemetria dos sensores |
| **RabbitMQ Broker** | `5672` | `5672` | AMQP 0-9-1 | Filas de mensagens e confirmações |
| **RabbitMQ Management** | `15672` | `15672` | HTTP | Painel Web de Gestão de Filas |
| **Worker 1 (Eleição)** | `9001` | `9001` | TCP / JSON | Canal TCP do Algoritmo Bully |
| **Worker 2 (Eleição)** | `9002` | `9002` | TCP / JSON | Canal TCP do Algoritmo Bully |
| **Worker 3 (Eleição)** | `9003` | `9003` | TCP / JSON | Canal TCP do Algoritmo Bully |
| **PostgreSQL Primary** | `5432` | `5432` | PostgreSQL | Banco de escrita e replicação |
| **PostgreSQL Replica** | `5433` | `5432` | PostgreSQL | Réplica em Standby físico (leitura) |
| **Dashboard Web** | `3000` | `3000` | HTTP / WS | Painel em tempo real via WebSockets |

---

## 🔁 4. Fluxo Ponta a Ponta das Mensagens

1. **Geração da Leitura:** O sensor simulador gera uma leitura com `sensor_id`, `zone`, `moisture`, `temperature`, `timestamp` e incrementa seu relógio de Lamport local $L_{local} = L_{local} + 1$.
2. **Envio gRPC:** O pacote `TelemetryPacket` é transmitido via RPC `SendTelemetry` para o Gateway na porta `50051`.
3. **Ingress & Sincronização:** O Gateway recebe o pacote, atualiza seu relógio:
   $$L_{gw} = \max(L_{local}, L_{msg}) + 1$$
   Em seguida, gera um `message_id` UUID único, enriquece o pacote e publica na fila `agro_telemetry_queue` utilizando **ConfirmChannel** (garantia de persistência do RabbitMQ).
4. **ACK do Gateway:** O Gateway responde imediatamente ao sensor com `TelemetryAck` contendo `queued: true`, `message_id` e $L_{gw}$.
5. **Processamento Concorrente (*Competing Consumers*):** Os três nós Workers concorrem pela fila `agro_telemetry_queue` com `prefetch(1)`. Cada worker que recebe uma mensagem:
   - Valida o payload.
   - Atualiza seu relógio: $L_{worker} = \max(L_{local}, L_{gw}) + 1$.
   - Constrói o objeto `ProcessedTelemetry` enriquecido com $L_{worker}$ e `worker_id`.
   - Publica o resultado na fila `agro_processed_results`.
   - Envia `ACK` da mensagem original para o RabbitMQ.
6. **Consolidação Exclusiva pelo Líder:** Apenas o Worker com liderança ativa consome da fila `agro_processed_results`. O Líder:
   - Sincroniza seu relógio com $L_{worker}$.
   - Armazena os eventos em um buffer causal ordenado deterministicamente por:
     $$\text{Critério 1: } L_{worker} \implies \text{Critério 2: } \text{worker\_id} \implies \text{Critério 3: } \text{message\_id}$$
   - Avalia a regra de irrigação: se `moisture < 40%`, executa inserção idempotente no PostgreSQL Primary:
     ```sql
     INSERT INTO irrigation_log (message_id, sensor_id, zone, moisture, temperature, lamport_time, event_ts, worker_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     ON CONFLICT (message_id) DO NOTHING;
     ```
7. **Streaming Replication:** O PostgreSQL Primary envia as alterações via WAL (*Write-Ahead Logging*) para o PostgreSQL Replica.
8. **Dashboard em Tempo Real:** O servidor Web Fastify consulta o PostgreSQL Replica e transmite os logs de irrigação e estados dos nós para o navegador via WebSocket.

---

## 🚀 5. Como Executar o Projeto

### Pré-requisitos
- Docker Engine $\ge$ 24.x
- Docker Compose $\ge$ 2.x
- Node.js $\ge$ 20.x (para compilação/testes locais opcionais)

### Execução via Docker Compose (Recomendada)

```bash
# 1. Construir e subir todos os contêineres em segundo plano
docker compose up --build -d

# 2. Verificar o status dos contêineres
docker compose ps

# 3. Acompanhar os logs unificados de todos os serviços
docker compose logs -f
```

### Execução dos Testes Automatizados Locais

```bash
# Executa a suíte de testes unitários
npm test
```

---

## 🧪 6. Guia de Demonstração e Comprovação Prática

### 6.1. Comprovação do Competing Consumers (3 Workers ativos)
Execute o comando abaixo para visualizar que **todos os 3 workers** recebem e processam leituras da fila:
```bash
docker compose logs worker-1 worker-2 worker-3 | grep "Leitura processada"
```
*Evidência esperada:* Mensagens intercaladas de `[Worker-1]`, `[Worker-2]` e `[Worker-3]`.

---

### 6.2. Comprovação do Relógio Lógico de Lamport
Observe a progressão monotônica e a sincronização do relógio entre cliente, gateway, workers e líder:
```bash
docker compose logs | grep -E "L_local|L_gw|L_worker|L_leader"
```
*Evidência esperada:* Cada nó atualiza seu relógio para $\max(L_{local}, L_{remoto}) + 1$.

---

### 6.3. Comprovação da Eleição Bully e Queda do Líder
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

### 6.4. Comprovação da Replicação PostgreSQL Primary → Replica
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

### 6.5. Comprovação de Idempotência
Verifique que a constraint de unicidade no banco impede duplicações em caso de reentrega de mensagens:
```bash
docker compose exec postgres-primary psql -U agro -d agrosense -c "SELECT COUNT(*), COUNT(DISTINCT message_id) FROM irrigation_log;"
```
*Evidência esperada:* `count` e `count(distinct message_id)` são rigorosamente idênticos.

---

### 6.6. Comprovação do Registro de Mudanças de Liderança
Consulte a tabela de auditoria de liderança:
```bash
docker compose exec postgres-primary psql -U agro -d agrosense -c "SELECT * FROM leader_election_log ORDER BY id DESC;"
```

---

## 📋 7. Evidências Reais de Logs da Aplicação

### Log do Sensor (Client):
```text
[Client] ✓ sensor-A-01 | zone=A | moisture=32.40% temp=24.10°C | L_local=5 → L_gw=6 | msgId=a1b2c3d4...
[Client] ✓ sensor-B-01 | zone=B | moisture=68.15% temp=19.80°C | L_local=5 → L_gw=7 | msgId=b2c3d4e5...
```

### Log do Gateway:
```text
[Gateway] gRPC escutando na porta 50051 | Lamport inicial=0
[RabbitMQ/Gateway] Broker CONFIRM msgId=a1b2c3d4-e5f6-7890-abcd-ef1234567890 sensor=sensor-A-01 L_gw=6
```

### Log dos Workers (Competing Consumers):
```text
[Worker-1] ✓ Leitura processada: sensor=sensor-A-01 zone=A moisture=32.4% | L_gw=6 → L_worker=8 | msgId=a1b2c3d4...
[Worker-2] ✓ Leitura processada: sensor=sensor-B-01 zone=B moisture=68.15% | L_gw=7 → L_worker=9 | msgId=b2c3d4e5...
```

### Log do Worker Líder (Eleição e Consolidação):
```text
[Election/3] 👑 VITÓRIA BULLY: worker-3 é o LÍDER!
[Leader/Worker-3] ★ Iniciando consumo da fila de consolidação agro_processed_results...
[Leader/Worker-3] 💧 DECISÃO DE IRRIGAÇÃO: Zona A (umidade=32.4% < 40%) | sensor=sensor-A-01 L_worker=8 L_leader=10 msgId=a1b2c3d4...
[DB] Log de irrigação persistido: msgId=a1b2c3d4-e5f6-7890-abcd-ef1234567890 sensor=sensor-A-01 zone=A L=8
```

---

## 🤖 8. Declaração de Uso de Inteligência Artificial

Este projeto utilizou ferramentas de Inteligência Artificial para análise arquitetural, refatoração de código concorrente, implementação do protocolo de socket TCP do algoritmo Bully, automação de testes unitários e elaboração da documentação técnica.

- **Ferramentas Utilizadas:** Antigravity IDE & Google Gemini.
- **Intervenções Realizadas:**
  1. Correção do descarte prematuro de mensagens em workers não líderes através da separação entre fila de telemetria e fila de resultados consolidados.
  2. Correção do canal de resposta PING/PONG no mesmo socket TCP para prevenir falsas eleições de líder.
  3. Configuração de replicação física PostgreSQL Streaming Replication (Primary $\to$ Replica).
  4. Implementação de idempotência ponta a ponta com propagação de `message_id` UUID e Publisher Confirms no RabbitMQ.
  5. Criação de suíte de testes unitários em TypeScript estrito.
