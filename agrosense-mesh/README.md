# 🌱 AgroSense Mesh

AgroSense Mesh é um sistema distribuído acadêmico desenvolvido para a disciplina de Programação Distribuída e Paralela (baseado no modelo "A Metrópole Resiliente"). O sistema processa e monitora dados de telemetria agrícola em larga escala, garantindo tolerância a falhas e consistência de dados em um ambiente onde falhas de rede e quedas de nós são esperadas.

## 🏗️ Arquitetura e Conceitos Implementados

O sistema implementa conceitos clássicos de Sistemas Distribuídos:

*   **Comunicação Indireta & Desacoplamento:** O envio de dados dos sensores não bloqueia o gateway. Foi utilizada uma fila de mensagens (**RabbitMQ**) para absorver picos de tráfego.
*   **Competing Consumers:** Múltiplos nós processadores (**Workers**) puxam dados da mesma fila concorrentemente. Se a carga aumentar, o trabalho é automaticamente balanceado. Se um nó cair, as mensagens não são perdidas e os outros assumem a carga.
*   **Eleição de Líder (Algoritmo do Valentão / Bully Election):** Embora todos os Workers processem a telemetria, **apenas o líder** eleito tem permissão para tomar ações críticas (como acionar o sistema de irrigação e gravar no banco de dados principal). Se o líder atual falhar (timeout/crash), os nós restantes elegem um novo líder de forma autônoma.
*   **Relógios Lógicos (Lamport):** Como não há garantia de relógios físicos sincronizados em sistemas distribuídos, os nós utilizam o Algoritmo de Lamport para sequenciar e ordenar causalmente as ativações do sistema de irrigação.
*   **Tolerância a Falhas e Replicação:** O banco de dados PostgreSQL está configurado em modelo Primário/Réplica. As escritas (irrigação) são feitas no Primário, e o Dashboard realiza leituras na Réplica, minimizando gargalos de concorrência.
*   **RPC (Remote Procedure Call):** Comunicação entre os sensores simulados e o Gateway via **gRPC** de alta performance.

## 🚀 Como Executar

**Pré-requisitos:** Docker e Docker Compose instalados.

1.  Abra o terminal na raiz do projeto.
2.  Suba toda a topologia de contêineres:
    ```bash
    docker compose up -d
    ```
3.  Acesse as interfaces:
    *   **Dashboard Visual em Tempo Real:** `http://localhost:3000`
    *   **Painel Administrativo do RabbitMQ:** `http://localhost:15672` (Usuário: `guest`, Senha: `guest`)

**Como simular a quebra de um nó (Teste de Reeleição):**
```bash
# Pare o container do Worker Líder (geralmente o worker-3, indicado pela 👑 no dashboard)
docker compose stop worker-3

# Observe o dashboard: o worker-3 ficará OFFLINE e, após alguns segundos, 
# a coroa passará para o worker-2.
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
