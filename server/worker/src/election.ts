import net from 'net';
import { EventEmitter } from 'events';

/**
 * BullyElection – implementa o Algoritmo do Valentão (Bully Election).
 *
 * Topologia:
 *  • Cada nó tem um NODE_ID numérico único (1, 2 ou 3).
 *  • Nós se comunicam via TCP nas portas 900{NODE_ID} (ex.: 9001, 9002, 9003).
 *
 * Protocolo de mensagens (JSON sobre TCP delimitado por \n):
 *  { type: "ELECTION",  from: number }  – Início/propagação de eleição
 *  { type: "OK",        from: number }  – Nó maior responde que está vivo
 *  { type: "LEADER",    from: number }  – Novo líder anuncia vitória
 *  { type: "HEARTBEAT", from: number }  – Líder sinaliza periodicamente que está vivo
 *  { type: "PING",      from: number }  – Verificação de saúde direta
 *  { type: "PONG",      from: number }  – Resposta de saúde enviada no mesmo socket
 */

export type ElectionMessage =
  | { type: 'ELECTION';  from: number }
  | { type: 'OK';        from: number }
  | { type: 'LEADER';    from: number }
  | { type: 'HEARTBEAT'; from: number }
  | { type: 'PING';      from: number }
  | { type: 'PONG';      from: number };

export class BullyElection extends EventEmitter {
  private readonly nodeId:          number;
  private readonly allNodes:        number[];
  private readonly basePort:        number = 9000;

  private leaderId:                 number | null = null;
  private electing:                 boolean       = false;
  private electionTimer:            NodeJS.Timeout | null = null;
  private server:                   net.Server;
  private heartbeatTimer:           NodeJS.Timeout | null = null;
  private leaderCheckTimer:         NodeJS.Timeout | null = null;
  private lastHeartbeatReceivedAt:  number = Date.now();

  constructor(nodeId: number, allNodes: number[]) {
    super();
    this.nodeId   = nodeId;
    this.allNodes = [...allNodes].sort((a, b) => a - b);
    this.server   = net.createServer(socket => this.handleConnection(socket));
  }

  /** Porta TCP de eleição de um nó */
  private portOf(id: number): number {
    return this.basePort + id;
  }

  /** Inicializa o servidor TCP e dispara a eleição inicial */
  async start(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.server.listen(this.portOf(this.nodeId), '0.0.0.0', () => {
        console.log(`[Election/${this.nodeId}] Servidor TCP escutando na porta ${this.portOf(this.nodeId)}`);
        resolve();
      });
      this.server.on('error', (err) => {
        console.error(`[Election/${this.nodeId}] Erro no servidor TCP:`, err);
        reject(err);
      });
    });

    // Aguarda estabilização da rede antes de disparar eleição
    await this.delay(1500 + this.nodeId * 300);
    this.startElection();

    // Monitor de saúde do líder a cada 5s
    this.leaderCheckTimer = setInterval(() => this.checkLeaderAlive(), 5000);
  }

  /** Trata conexões TCP de entrada */
  private handleConnection(socket: net.Socket): void {
    let buffer = '';

    socket.on('data', (chunk) => {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';

      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const msg: ElectionMessage = JSON.parse(line);
          this.handleMessage(msg, socket);
        } catch {
          // ignora linhas malformadas
        }
      }
    });

    socket.on('error', () => {
      // conexão cliente pode fechar abruptamente
    });
  }

  /** Despacha mensagens de eleição recebidas */
  private handleMessage(msg: ElectionMessage, socket: net.Socket): void {
    switch (msg.type) {
      case 'ELECTION':
        console.log(`[Election/${this.nodeId}] Recebido ELECTION de worker-${msg.from}`);
        // Bully: se meu ID for maior, respondo OK para o nó solicitante e inicio minha eleição
        if (this.nodeId > msg.from) {
          this.send(msg.from, { type: 'OK', from: this.nodeId });

          if (!this.electing) {
            this.startElection();
          }
        }
        break;

      case 'OK':
        // Um nó de ID superior está vivo – cancelo minha candidatura
        if (msg.from > this.nodeId) {
          console.log(`[Election/${this.nodeId}] Recebido OK de worker-${msg.from} – nó maior ativo. Cancelando candidatura.`);
          this.electing = false;
          if (this.electionTimer) {
            clearTimeout(this.electionTimer);
            this.electionTimer = null;
          }
        }
        break;

      case 'LEADER': {
        // Invariante Bully: nó com ID superior NUNCA aceita nó inferior como líder
        if (msg.from < this.nodeId) {
          console.warn(`[Election/${this.nodeId}] Rejeitando LEADER de nó inferior worker-${msg.from}. Assumindo liderança / disparando eleição.`);
          this.startElection();
          break;
        }

        const previousLeader = this.leaderId;
        this.leaderId = msg.from;
        this.electing = false;
        this.lastHeartbeatReceivedAt = Date.now();
        if (this.electionTimer) {
          clearTimeout(this.electionTimer);
          this.electionTimer = null;
        }

        console.log(`[Election/${this.nodeId}] ★ Novo líder reconhecido: worker-${msg.from}`);

        if (this.nodeId === msg.from) {
          this.startHeartbeat();
        } else {
          this.stopHeartbeat();
        }

        if (previousLeader !== this.leaderId) {
          this.emit('leader-changed', this.leaderId);
        }
        break;
      }

      case 'HEARTBEAT':
        // Invariante Bully: rejeita heartbeat de nó inferior
        if (msg.from < this.nodeId) {
          console.warn(`[Election/${this.nodeId}] Rejeitando HEARTBEAT de nó inferior worker-${msg.from}. Disparando eleição.`);
          this.startElection();
          break;
        }
        if (this.leaderId !== msg.from) {
          const previousLeader = this.leaderId;
          this.leaderId = msg.from;
          if (previousLeader !== this.leaderId) {
            this.emit('leader-changed', this.leaderId);
          }
        }
        this.lastHeartbeatReceivedAt = Date.now();
        break;

      case 'PING':
        // Responde PONG no MESMO socket TCP recebido
        if (socket && !socket.destroyed && socket.writable) {
          try {
            socket.write(JSON.stringify({ type: 'PONG', from: this.nodeId }) + '\n');
          } catch {
            // falha ao escrever no socket
          }
        }
        break;

      case 'PONG':
        // PONG recebido fora de fluxo normal
        break;
    }
  }

  /**
   * Inicia uma rodada de eleição pelo Algoritmo do Valentão (Bully).
   * Envia ELECTION para todos os nós com ID superior.
   * Se nenhum responder com OK dentro do timeout, declara-se líder.
   */
  public startElection(): void {
    if (this.electing) return;
    this.electing = true;
    console.log(`[Election/${this.nodeId}] Iniciando rodada de eleição Bully...`);

    const higherNodes = this.allNodes.filter(n => n > this.nodeId);

    if (higherNodes.length === 0) {
      // Sou o maior nó do cluster – vitória imediata
      this.declareLeader();
      return;
    }

    // Envia ELECTION para todos os nós maiores
    for (const id of higherNodes) {
      this.send(id, { type: 'ELECTION', from: this.nodeId });
    }

    // Aguarda respostas OK por 2000ms
    if (this.electionTimer) clearTimeout(this.electionTimer);
    this.electionTimer = setTimeout(() => {
      if (this.electing) {
        console.log(`[Election/${this.nodeId}] Nenhum nó superior respondeu OK. Assumindo liderança.`);
        this.declareLeader();
      }
    }, 2000);
  }

  /** Declara este nó como líder e notifica todos os outros nós */
  private declareLeader(): void {
    const previousLeader = this.leaderId;
    this.leaderId = this.nodeId;
    this.electing = false;
    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }

    console.log(`[Election/${this.nodeId}] 👑 VITÓRIA BULLY: worker-${this.nodeId} é o LÍDER!`);

    const others = this.allNodes.filter(n => n !== this.nodeId);
    for (const id of others) {
      this.send(id, { type: 'LEADER', from: this.nodeId });
    }

    this.startHeartbeat();

    if (previousLeader !== this.nodeId) {
      this.emit('leader-changed', this.nodeId);
    }
  }

  /** Líder envia heartbeats periódicos a cada 3s */
  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      if (this.leaderId !== this.nodeId) {
        this.stopHeartbeat();
        return;
      }
      const others = this.allNodes.filter(n => n !== this.nodeId);
      for (const id of others) {
        this.send(id, { type: 'HEARTBEAT', from: this.nodeId });
      }
    }, 3000);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  /** Verifica se o líder está vivo via ping ou heartbeat recente */
  private async checkLeaderAlive(): Promise<void> {
    if (this.leaderId === null || this.leaderId === this.nodeId) {
      if (this.leaderId === null && !this.electing) {
        this.startElection();
      }
      return;
    }

    // Se recebemos heartbeat há menos de 5s, consideramos saudável
    const timeSinceLastHb = Date.now() - this.lastHeartbeatReceivedAt;
    if (timeSinceLastHb < 5000) {
      return;
    }

    // Testa disponibilidade via PING direto
    const alive = await this.ping(this.leaderId);
    if (!alive) {
      console.warn(`[Election/${this.nodeId}] Líder worker-${this.leaderId} inalcançável (timeout PING). Disparando nova eleição.`);
      this.leaderId = null;
      this.startElection();
    } else {
      this.lastHeartbeatReceivedAt = Date.now();
    }
  }

  /** Envia um PING no socket e aguarda a resposta PONG com timeout estrito */
  public ping(targetId: number): Promise<boolean> {
    return new Promise((resolve) => {
      const socket = new net.Socket();
      let resolved = false;

      const cleanup = (result: boolean) => {
        if (resolved) return;
        resolved = true;
        clearTimeout(timeout);
        socket.removeAllListeners();
        socket.destroy();
        resolve(result);
      };

      const timeout = setTimeout(() => {
        cleanup(false);
      }, 2000);

      socket.connect(this.portOf(targetId), `worker-${targetId}`, () => {
        try {
          socket.write(JSON.stringify({ type: 'PING', from: this.nodeId }) + '\n');
        } catch {
          cleanup(false);
        }
      });

      socket.on('data', (data) => {
        try {
          const lines = data.toString().split('\n');
          for (const line of lines) {
            if (!line.trim()) continue;
            const msg = JSON.parse(line.trim());
            if (msg.type === 'PONG' && msg.from === targetId) {
              cleanup(true);
              return;
            }
          }
        } catch {
          // ignora JSON incompleto
        }
      });

      socket.on('error', () => {
        cleanup(false);
      });

      socket.on('close', () => {
        cleanup(false);
      });
    });
  }

  /** Envia mensagem JSON unidirecional via TCP para outro worker */
  private send(targetId: number, msg: ElectionMessage): void {
    const socket = new net.Socket();
    socket.setTimeout(2000);

    socket.connect(this.portOf(targetId), `worker-${targetId}`, () => {
      try {
        socket.end(JSON.stringify(msg) + '\n');
      } catch {
        socket.destroy();
      }
    });

    socket.on('error', () => {
      socket.destroy();
    });

    socket.on('timeout', () => {
      socket.destroy();
    });
  }

  public stop(): void {
    this.stopHeartbeat();
    if (this.leaderCheckTimer) clearInterval(this.leaderCheckTimer);
    if (this.electionTimer) clearTimeout(this.electionTimer);
    this.server.close();
  }

  private delay(ms: number): Promise<void> {
    return new Promise(res => setTimeout(res, ms));
  }

  get currentLeader(): number | null { return this.leaderId; }
  get isLeader():      boolean        { return this.leaderId === this.nodeId; }
}

