/**
 * LamportClock – Relógio Lógico de Lamport (compartilhado entre módulos).
 * Regras: L_local = max(L_local, L_msg) + 1 ao receber mensagem.
 */
export class LamportClock {
  private _clock: number = 0;

  get value(): number { return this._clock; }

  tick(): number { return ++this._clock; }

  receive(received: number): number {
    this._clock = Math.max(this._clock, received) + 1;
    return this._clock;
  }
}
