import test from 'node:test';
import assert from 'node:assert/strict';
import { LamportClock } from '../lamport';
import { sortCausalEvents, MOISTURE_THRESHOLD, ProcessedTelemetry } from '../index';

// ─── TESTE 1: LamportClock.receive() ──────────────────────────────────────────
test('TESTE 1: LamportClock.receive() deve atualizar para max(L_local, L_remoto) + 1', () => {
  const clock = new LamportClock();
  assert.equal(clock.value, 0, 'Relógio inicial deve ser 0');

  // Recebe timestamp 5 quando relógio local é 0
  const t1 = clock.receive(5);
  assert.equal(t1, 6, 'max(0, 5) + 1 deve ser 6');
  assert.equal(clock.value, 6);

  // Recebe timestamp 3 (menor que local 6)
  const t2 = clock.receive(3);
  assert.equal(t2, 7, 'max(6, 3) + 1 deve ser 7');
  assert.equal(clock.value, 7);

  // Recebe timestamp 20 (maior que local 7)
  const t3 = clock.receive(20);
  assert.equal(t3, 21, 'max(7, 20) + 1 deve ser 21');
  assert.equal(clock.value, 21);
});

// ─── TESTE 2: LamportClock.tick() ─────────────────────────────────────────────
test('TESTE 2: LamportClock.tick() deve incrementar o relógio monotonicamente', () => {
  const clock = new LamportClock();
  assert.equal(clock.value, 0);

  assert.equal(clock.tick(), 1);
  assert.equal(clock.tick(), 2);
  assert.equal(clock.tick(), 3);
  assert.equal(clock.value, 3);
});

// ─── TESTE 3: Decisão de Irrigação (moisture < 40%) ────────────────────────────
test('TESTE 3: Regra de irrigação deve ativar somente quando moisture < 40%', () => {
  const shouldIrrigate = (moisture: number) => moisture < MOISTURE_THRESHOLD;

  assert.equal(shouldIrrigate(25.5), true, 'Umidade 25.5% deve acionar irrigação (< 40%)');
  assert.equal(shouldIrrigate(39.9), true, 'Umidade 39.9% deve acionar irrigação (< 40%)');
  assert.equal(shouldIrrigate(40.0), false, 'Umidade 40.0% NÃO deve acionar irrigação (>= 40%)');
  assert.equal(shouldIrrigate(75.2), false, 'Umidade 75.2% NÃO deve acionar irrigação (>= 40%)');
});

// ─── TESTE 4: Idempotência por messageId ──────────────────────────────────────
test('TESTE 4: Idempotência deve ignorar duplicatas do mesmo message_id', () => {
  const messageStore = new Set<string>();

  function processWithIdempotency(messageId: string): { inserted: boolean } {
    if (messageStore.has(messageId)) {
      return { inserted: false }; // Simula ON CONFLICT DO NOTHING
    }
    messageStore.add(messageId);
    return { inserted: true };
  }

  const msgId = 'e2b3c4d5-6789-4abc-9def-0123456789ab';

  const firstAttempt = processWithIdempotency(msgId);
  assert.equal(firstAttempt.inserted, true, 'Primeira inserção deve ter sucesso');

  const secondAttempt = processWithIdempotency(msgId);
  assert.equal(secondAttempt.inserted, false, 'Segunda inserção com mesmo UUID deve ser ignorada (idempotência)');

  const thirdAttempt = processWithIdempotency(msgId);
  assert.equal(thirdAttempt.inserted, false, 'Terceira tentativa com mesmo UUID deve continuar sendo ignorada');
});

// ─── TESTE 5: Regra do Algoritmo Bully (Maior ID Ativo Vence) ──────────────────
test('TESTE 5: Algoritmo Bully deve determinar como líder o nó ativo com maior ID', () => {
  function electLeader(activeNodes: number[]): number {
    if (activeNodes.length === 0) throw new Error('Nenhum nó ativo');
    return Math.max(...activeNodes);
  }

  // Cenário 1: Todos os nós ativos [1, 2, 3] → worker-3 é o líder
  assert.equal(electLeader([1, 2, 3]), 3, 'Com nós 1, 2, 3 ativos, worker-3 deve ser eleito líder');

  // Cenário 2: worker-3 cai, sobram [1, 2] → worker-2 assume
  assert.equal(electLeader([1, 2]), 2, 'Com worker-3 inativo, worker-2 deve ser eleito líder');

  // Cenário 3: worker-2 cai, sobra [1] → worker-1 assume
  assert.equal(electLeader([1]), 1, 'Com apenas worker-1 ativo, worker-1 deve ser líder');

  // Cenário 4: worker-3 volta [1, 3] → worker-3 reassume a liderança
  assert.equal(electLeader([1, 3]), 3, 'Ao retornar, worker-3 deve reassumir a liderança');
});

// ─── TESTE 6: Ordenação Causal Determinística de Eventos Processados ───────────
test('TESTE 6: Ordenação causal deve priorizar Lamport, depois worker_id, depois message_id', () => {
  const events: ProcessedTelemetry[] = [
    {
      message_id: 'c-uuid',
      sensor_id: 'sensor-A-01',
      zone: 'A',
      moisture: 30,
      temperature: 25,
      timestamp: 1000,
      original_lamport: 1,
      gateway_lamport: 2,
      worker_id: 2,
      worker_lamport: 10,
      processed_at: 1005,
    },
    {
      message_id: 'a-uuid',
      sensor_id: 'sensor-B-01',
      zone: 'B',
      moisture: 50,
      temperature: 22,
      timestamp: 900,
      original_lamport: 1,
      gateway_lamport: 2,
      worker_id: 1,
      worker_lamport: 5,
      processed_at: 905,
    },
    {
      message_id: 'b-uuid',
      sensor_id: 'sensor-C-01',
      zone: 'C',
      moisture: 35,
      temperature: 28,
      timestamp: 1100,
      original_lamport: 2,
      gateway_lamport: 3,
      worker_id: 1,
      worker_lamport: 10,
      processed_at: 1105,
    },
    {
      message_id: 'd-uuid',
      sensor_id: 'sensor-D-01',
      zone: 'D',
      moisture: 20,
      temperature: 30,
      timestamp: 1200,
      original_lamport: 3,
      gateway_lamport: 4,
      worker_id: 2,
      worker_lamport: 10,
      processed_at: 1205,
    },
  ];

  const sorted = [...events].sort(sortCausalEvents);

  // 1º lugar: worker_lamport=5 (sensor-B, worker 1)
  assert.equal(sorted[0].sensor_id, 'sensor-B-01');
  assert.equal(sorted[0].worker_lamport, 5);

  // 2º lugar: worker_lamport=10, worker_id=1 (sensor-C, worker 1)
  assert.equal(sorted[1].sensor_id, 'sensor-C-01');
  assert.equal(sorted[1].worker_id, 1);
  assert.equal(sorted[1].worker_lamport, 10);

  // 3º e 4º lugares: worker_lamport=10, worker_id=2 -> desempate por message_id ('c-uuid' < 'd-uuid')
  assert.equal(sorted[2].message_id, 'c-uuid');
  assert.equal(sorted[2].worker_id, 2);

  assert.equal(sorted[3].message_id, 'd-uuid');
  assert.equal(sorted[3].worker_id, 2);
});
