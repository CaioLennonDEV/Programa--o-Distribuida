/**
 * LamportClock – implementação do Relógio Lógico de Lamport.
 *
 * Regras:
 *   • evento interno → L = L + 1
 *   • envio de mensagem → inclui L = L + 1 no pacote
 *   • recebimento de mensagem com timestamp T → L = max(L, T) + 1
 */
export class LamportClock {
  private _clock: number = 0;

  /** Valor atual do relógio (somente leitura externa) */
  get value(): number {
    return this._clock;
  }

  /**
   * Incrementa o relógio para um evento interno ou envio.
   * @returns novo valor do relógio
   */
  tick(): number {
    this._clock += 1;
    return this._clock;
  }

  /**
   * Atualiza o relógio ao receber uma mensagem com timestamp externo.
   * Implementa: L = max(L_local, L_msg) + 1
   * @param received  timestamp de Lamport recebido na mensagem
   * @returns novo valor do relógio após sincronização
   */
  receive(received: number): number {
    this._clock = Math.max(this._clock, received) + 1;
    return this._clock;
  }
}
