import amqp, { ChannelModel, Channel } from 'amqplib';
import { randomUUID } from 'crypto';

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';
const QUEUE_NAME   = 'agro_telemetry_queue';

export interface PublishResult {
  messageId: string;
  success:   boolean;
}

export class RabbitMQPublisher {
  private connection: ChannelModel | null = null;
  private channel:    Channel      | null = null;

  async connect(): Promise<void> {
    const MAX_RETRIES = 10;
    const DELAY_MS    = 3000;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        this.connection = await amqp.connect(RABBITMQ_URL);
        this.channel    = await this.connection.createChannel();

        // Fila durável – sobrevive a reinícios do broker
        await this.channel.assertQueue(QUEUE_NAME, { durable: true });

        console.log(`[RabbitMQ] Conectado em ${RABBITMQ_URL} | Fila: ${QUEUE_NAME}`);

        this.connection.on('error',  (err) => console.error('[RabbitMQ] Erro de conexão:', err));
        this.connection.on('close',  ()    => console.warn('[RabbitMQ] Conexão encerrada – reconectando...'));

        return;
      } catch (err) {
        console.warn(`[RabbitMQ] Tentativa ${attempt}/${MAX_RETRIES} falhou. Aguardando ${DELAY_MS}ms...`);
        await new Promise(res => setTimeout(res, DELAY_MS));
      }
    }

    throw new Error('[RabbitMQ] Não foi possível estabelecer conexão após todas as tentativas.');
  }

  /**
   * Publica uma mensagem de telemetria na fila principal.
   * A flag `persistent: true` garante que a mensagem survive a reinícios do broker.
   */
  async publish(payload: Record<string, unknown>): Promise<PublishResult> {
    const messageId = randomUUID();

    if (!this.channel) {
      console.error('[RabbitMQ] Canal não disponível.');
      return { messageId, success: false };
    }

    try {
      const buffer = Buffer.from(JSON.stringify(payload));

      const success = this.channel.sendToQueue(QUEUE_NAME, buffer, {
        persistent:  true,
        messageId,
        contentType: 'application/json',
        timestamp:   Math.floor(Date.now() / 1000),
      });

      if (success) {
        console.log(`[RabbitMQ] Publicado msgId=${messageId} sensor=${payload['sensor_id']}`);
      }

      return { messageId, success };
    } catch (err) {
      console.error('[RabbitMQ] Erro ao publicar:', err);
      return { messageId, success: false };
    }
  }

  async close(): Promise<void> {
    await this.channel?.close();
    await this.connection?.close();
  }
}
