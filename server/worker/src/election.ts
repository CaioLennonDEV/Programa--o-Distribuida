import net from 'net';
import { EventEmitter } from 'events';

/**
 * BullyElection – implementa o Algoritmo do Valentão (Bully Election).
 *
 * Topologia:
 *  • Cada nó tem um NODE_ID numérico único (1, 2 ou 3).
 *  • Nós se comunicam via TCP nas portas 900{NODE_ID} (ex.: 9001, 9002, 9003).
 *
 * Protocolo de mensagens (JSON sobre TCP):
 *  { type: "ELECTION", from: number }   – Início/propagação de eleição
 *  { type: "OK",       from: number }   – Nó maior responde que está vivo
 *  { type: "LEADER",   from: number }   – Novo líder anuncia vitória
 *  { type: "HEARTBEAT",from: number }   – Líder sinaliza que está vivo
 *  { type: "PING",     from: number }   – Verificação de saúde
 *  { type: "PONG",     from: number }   – Resposta de saúde
 */

export type ElectionMessage =
  | { type: 'ELECTION';  from: number }
  | { type: 'OK';        from: number }
  | { type: 'LEADER';    from: number }
  | { type: 'HEARTBEAT'; from: number }
  | { type: 'PING';      from: number }
  | { type: 'PONG';      from: number };

export class BullyElection extends EventEmitter {
  private readonly nodeId:    number;
  private readonly allNodes:  number[];
  private readonly basePort:  number = 9000;

  private leaderId:           number | null = null;
  private electing:           boolean       = false;
  private server:             net.Server;
  private heartbeatTimer:     NodeJS.Timeout | null = null;
  private leaderCheckTimer:   NodeJS.Timeout | null = null;

  constructor(nodeId: number, allNodes: number[]) {
    super();
    this.nodeId   = nodeId;
    this.allNodes = allNodes.sort((a, b) => a - b);
    this.server   = net.createServer(socket => this.handleConnection(socket));
  }

  /** Porta TCP de eleição de um nó */
  private portOf(id: number): number {
    return this.basePort + id;
  }

  /** Inicializa o servidor TCP e dispara uma eleição inicial */
  async start(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.server.listen(this.portOf(this.nodeId), '0.0.0.0', () => {
        console.log(`[Election/${this.nodeId}] Servidor TCP escutando na porta ${this.portOf(this.nodeId)}`);
        resolve();
      });
      this.server.on('error', reject);
    });

    // Aguarda outros nós iniciarem antes de disparar a eleição
    await this.delay(2000 + this.nodeId * 500);
    this.startElection();

    // Monitor de heartbeat do líder
    this.leaderCheckTimer = setInterval(() => this.checkLeaderAlive(), 8000);
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
          // ignora mensagens malformadas
        }
      }
    });

    socket.on('error', () => { /* silencia erros de socket */ });
  }

  /** Despacha mensagens de eleição recebidas */
  private handleMessage(msg: ElectionMessage, socket: net.Socket): void {
    console.log(`[Election/${this.nodeId}] Recebido: ${JSON.stringify(msg)}`);

    switch (msg.type) {
      case 'ELECTION':
        // Bully: se eu tenho ID maior, respondo OK e inicio minha própria eleição
        if (msg.from < this.nodeId) {
          this.send(msg.from, { type: 'OK', from: this.nodeId });
          if (!this.electing) this.startElection();
        }
        break;

      case 'OK':
        // Alguém maior está vivo — cancelo minha candidatura
        this.electing = false;
        console.log(`[Election/${this.nodeId}] Recebi OK de ${msg.from} – não serei líder`);
        break;

      case 'LEADER':
        // Novo líder anunciado
        this.leaderId  = msg.from;
        this.electing  = false;
        console.log(`[Election/${this.nodeId}] Novo líder: ${msg.from}`);
        this.emit('leader-changed', msg.from);
        if (msg.from === this.nodeId) this.startHeartbeat();
        break;

      case 'HEARTBEAT':
        // Líder está vivo — reseta o timer de verificação
        this.leaderId = msg.from;
        break;

      case 'PING':
        this.send(msg.from, { type: 'PONG', from: this.nodeId });
        break;
    }
  }

  /**
   * Inicia uma rodada de eleição pelo Algoritmo do Valentão.
   * Envia ELECTION para todos os nós com ID maior.
   * Se nenhum responder com OK no timeout, se declara líder.
   */
  private startElection(): void {
    if (this.electing) return;
    this.electing = true;
    console.log(`[Election/${this.nodeId}] Iniciando eleição...`);

    const higherNodes = this.allNodes.filter(n => n > this.nodeId);

    if (higherNodes.length === 0) {
      // Sou o nó de maior ID — me declaro líder imediatamente
      this.declareLeader();
      return;
    }

    // Envia ELECTION para todos os nós maiores
    for (const id of higherNodes) {
      this.send(id, { type: 'ELECTION', from: this.nodeId });
    }

    // Se nenhum OK chegar em 3 s, me declaro líder
    setTimeout(() => {
      if (this.electing) this.declareLeader();
    }, 3000);
  }

  /** Se declara líder e anuncia para todos os demais nós */
  private declareLeader(): void {
    this.leaderId = this.nodeId;
    this.electing = false;
    console.log(`[Election/${this.nodeId}] ★ Sou o LÍDER agora!`);

    const others = this.allNodes.filter(n => n !== this.nodeId);
    for (const id of others) {
      this.send(id, { type: 'LEADER', from: this.nodeId });
    }

    this.emit('leader-changed', this.nodeId);
    this.startHeartbeat();
  }

  /** O líder envia heartbeats periódicos para os demais nós */
  private startHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      if (this.leaderId !== this.nodeId) { clearInterval(this.heartbeatTimer!); return; }
      const others = this.allNodes.filter(n => n !== this.nodeId);
      for (const id of others) this.send(id, { type: 'HEARTBEAT', from: this.nodeId });
    }, 4000);
  }

  /** Verifica se o líder atual ainda está vivo; se não, inicia nova eleição */
  private checkLeaderAlive(): void {
    if (this.leaderId === null || this.leaderId === this.nodeId) return;

    this.ping(this.leaderId)
      .then(alive => { if (!alive) { console.warn(`[Election/${this.nodeId}] Líder ${this.leaderId} não responde – iniciando eleição`); this.leaderId = null; this.startElection(); } })
      .catch(() => { this.leaderId = null; this.startElection(); });
  }

  /** Envia um PING e aguarda PONG do nó alvo */
  private ping(targetId: number): Promise<boolean> {
    return new Promise((resolve) => {
      const socket = new net.Socket();
      const timeout = setTimeout(() => { socket.destroy(); resolve(false); }, 2000);

      socket.connect(this.portOf(targetId), `worker-${targetId}`, () => {
        socket.write(JSON.stringify({ type: 'PING', from: this.nodeId }) + '\n');
      });

      socket.on('data', (data) => {
        try {
          const msg = JSON.parse(data.toString().trim());
          if (msg.type === 'PONG') { clearTimeout(timeout); socket.destroy(); resolve(true); }
        } catch { /* ignora */ }
      });

      socket.on('error', () => { clearTimeout(timeout); resolve(false); });
    });
  }

  /** Envia uma mensagem JSON via TCP para o nó alvo */
  private send(targetId: number, msg: ElectionMessage): void {
    const socket = new net.Socket();

    socket.connect(this.portOf(targetId), `worker-${targetId}`, () => {
      socket.write(JSON.stringify(msg) + '\n');
      socket.end();
    });

    socket.on('error', () => {
      // Nó alvo pode estar offline — silencia o erro
    });
  }

  private delay(ms: number): Promise<void> {
    return new Promise(res => setTimeout(res, ms));
  }

  get currentLeader(): number | null { return this.leaderId; }
  get isLeader():      boolean        { return this.leaderId === this.nodeId; }
}
