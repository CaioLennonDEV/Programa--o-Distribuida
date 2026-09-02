# 🌿 AgroSense Mesh

**Sistema Distribuído de Telemetria Agrícola**
Disciplina: Programação Distribuída e Paralela — Modelo "A Metrópole Resiliente"

---

## 📐 Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AgroSense Mesh Topology                      │
│                                                                     │
│  [Sensor-A] [Sensor-B] [Sensor-C] [Sensor-D] [Sensor-E]            │
│       │         │         │         │         │                     │
│       └─────────┴────┬────┴─────────┴─────────┘                    │
│                      │  gRPC (TelemetryPacket + Lamport)            │
│              ┌───────▼───────┐                                      │
│              │   GATEWAY     │ ← porta 50051                        │
│              │  (gRPC + ACK) │                                      │
│              └───────┬───────┘                                      │
│                      │  AMQP publish                                │
│              ┌───────▼────────────────┐                             │
│              │      RabbitMQ          │ ← 5672 / UI:15672           │
│              │  agro_telemetry_queue  │                             │
│              └──┬──────────┬──────────┘                             │
│                 │          │          │ Competing Consumers         │
│           ┌─────▼──┐ ┌─────▼──┐ ┌────▼───┐                         │
│           │Worker-1│ │Worker-2│ │Worker-3│ ← eleição Bully (TCP)   │
│           │Lamport │ │Lamport │ │Lamport │                          │
│           └────┬───┘ └───┬────┘ └───┬────┘                         │
│                │Líder persiste        │                             │
│          ┌─────▼──────────────────────┐                             │
│          │   PostgreSQL Primary       │ ← porta 5432                │
│          │   PostgreSQL Replica       │ ← porta 5433                │
│          └────────────────────────────┘                             │
│                                                                     │
│          ┌──────────────────────────────────┐                       │
│          │    Web Dashboard (Fastify+WS)     │ ← porta 3000         │
│          └──────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Inicialização

### Pré-requisitos
- Docker ≥ 24.x
- Docker Compose ≥ 2.x

### Subir toda a topologia

```bash
cd agrosense-mesh

# Construir e subir todos os serviços
docker compose up --build -d

# Acompanhar logs em tempo real
docker compose logs -f
```

### Acessar os serviços

| Serviço               | URL/Endpoint               |
|-----------------------|----------------------------|
| Dashboard Web         | http://localhost:3000       |
| RabbitMQ Management   | http://localhost:15672      |
| Gateway gRPC          | localhost:50051             |
| PostgreSQL Primary    | localhost:5432              |
| PostgreSQL Replica    | localhost:5433              |

Credenciais RabbitMQ: `guest` / `guest`
Credenciais PostgreSQL: `agro` / `agro123`

---

## 🧪 Testando Falhas e Reeleição de Líder

### 1. Derrubar o Worker Líder

```bash
# Descobrir quem é o líder atual (nos logs):
docker compose logs worker-1 worker-2 worker-3 | grep "LÍDER"

# Supondo que worker-3 é o líder — derrubá-lo:
docker compose stop worker-3

# Observar nos logs dos outros workers a reeleição automática:
docker compose logs -f worker-1 worker-2
```

**Comportamento esperado:**
- `worker-1` e `worker-2` detectam a ausência de heartbeat do líder após ~8s
- Disparam mensagens `ELECTION` entre si
- `worker-2` (ID maior) vence e envia `LEADER` para `worker-1`
- Dashboard web atualiza o badge para `LEADER`

### 2. Restaurar o worker derrubado

```bash
docker compose start worker-3

# Worker-3 se reconecta, detecta o novo líder e assume role ACTIVE
docker compose logs -f worker-3
```

### 3. Simular falha do Gateway

```bash
docker compose stop gateway

# Sensores recebem erro de conexão gRPC
# Subir novamente:
docker compose start gateway
```

### 4. Verificar logs de irrigação no banco

```bash
# Conectar ao banco primário
docker compose exec postgres-primary psql -U agro -d agrosense

# Consultar logs
SELECT * FROM recent_irrigation;
SELECT COUNT(*) FROM irrigation_log;
\q
```

### 5. Inspecionar fila RabbitMQ

```bash
# Via CLI
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

---

## 🔬 Conceitos Implementados

| Conceito                     | Onde                                  |
|------------------------------|---------------------------------------|
| Relógio de Lamport           | `gateway/src/lamport.ts`, `worker/src/lamport.ts` |
| Competing Consumers (AMQP)   | `worker/src/index.ts` (prefetch=1)    |
| ACK/NACK com requeue         | `worker/src/index.ts`                 |
| Algoritmo do Valentão (Bully)| `worker/src/election.ts`              |
| Fila durável                 | RabbitMQ + `durable: true`            |
| Mensagens persistentes       | `persistent: true` no publish         |
| Ordenação causal             | Buffer `causalBuffer` por L_local     |
| Persistência replicada       | PG Primary (escrita) + Replica (leitura dashboard) |
| Dashboard tempo real         | Fastify + WebSocket broadcast         |
| Server-Streaming gRPC        | `WatchWorkerStatus` RPC               |

---

## 📁 Estrutura de Arquivos

```
agrosense-mesh/
├── agro_telemetry.proto            # Contrato gRPC
├── docker-compose.yml              # Orquestração completa
├── .dockerignore
├── .gitignore
│
├── infra/
│   └── pg-init.sql                 # Schema PostgreSQL
│
├── server/
│   ├── gateway/                    # Módulo Gateway gRPC
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   └── src/
│   │       ├── index.ts            # Servidor gRPC principal
│   │       ├── lamport.ts          # Relógio de Lamport
│   │       └── rabbitmq.ts         # Publisher AMQP
│   │
│   └── worker/                     # Módulo Worker (3 instâncias)
│       ├── Dockerfile
│       ├── package.json
│       ├── tsconfig.json
│       └── src/
│           ├── index.ts            # Consumer AMQP + lógica principal
│           ├── lamport.ts          # Relógio de Lamport
│           ├── election.ts         # Algoritmo do Valentão (Bully)
│           └── database.ts         # Persistência PostgreSQL
│
├── client/                         # Simulador de Sensores
│   ├── Dockerfile
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       └── index.ts                # 5 sensores concorrentes via gRPC
│
└── web/                            # Dashboard em tempo real
    ├── Dockerfile
    ├── package.json
    ├── tsconfig.json
    └── src/
        ├── index.ts                # Servidor Fastify + WebSocket
        └── public/
            ├── index.html          # UI do dashboard
            ├── css/
            │   └── styles.css
            └── js/
                └── app.js          # Lógica frontend WS
```

---

## 🤖 Declaração de Uso de Inteligência Artificial

Este projeto foi construído utilizando ferramentas de Inteligência Artificial para estruturação da arquitetura base, geração de boilerplate, resolução de bugs em tempo de execução de contêineres e aprimoramento didático da interface gráfica.

*   **Agentes e Modelos Utilizados:** Antigravity IDE & Google Gemini.

### Prompt Original Base Utilizado

O desenvolvimento foi guiado a partir do seguinte prompt inserido no agente de IA:

> "Você é um Arquiteto de Software Especialista em Sistemas Distribuídos e TypeScript/Node.js.
> Preciso criar a estrutura base modular para o projeto acadêmico AgroSense Mesh (Disciplina de Programação Distribuída e Paralela, modelo 'A Metrópole Resiliente'). O sistema lida com telemetria agrícola distribuída, filas de mensagens, concorrência, relógios lógicos e eleição de líder.
> Gere a estrutura completa de código dividida conceitualmente em Server (Gateway gRPC + Workers com RabbitMQ), Client (Simulador de Sensores / Ingress), Web (Dashboard visual simples de monitoramento de nós/irrigação) e Banco (Persistência Replicada com banco primário e réplica via Docker)."

Os refinamentos subsequentes envolveram correções de compilação TypeScript com `strict: true`, acertos no roteamento de DNS interno do Docker para os sockets de TCP (Algoritmo Bully) e implementação de websockets do Fastify v10 no frontend.
