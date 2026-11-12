import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
} from "@nestjs/common";
import { Kafka, Producer, CompressionTypes } from "kafkajs";

/**
 * KAFKA PRODUCER SERVICE
 *
 * Wraps KafkaJS to publish domain events to Kafka topics.
 * All microservices subscribe to relevant topics and react independently.
 *
 * Topics used in this system:
 *   orders.placed        → payment-service, restaurant-service, dispatch-service
 *   orders.status_updated → order-service (updates the read model)
 *   payments.completed   → order-service, restaurant-service
 *   payments.failed      → order-service (cancels order)
 *
 * NOTE: If Kafka is not running the app still starts — Kafka events are
 * skipped with a warning. Install Kafka via: brew install kafka
 */
@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private producer: Producer;
  private connected = false;

  private readonly kafka = new Kafka({
    clientId: "order-service",
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
    retry: { initialRetryTime: 300, retries: 2 },
  });

  async onModuleInit() {
    this.producer = this.kafka.producer({
      allowAutoTopicCreation: false,
      transactionTimeout: 30000,
    });
    try {
      await this.producer.connect();
      this.connected = true;
      this.logger.log("Kafka producer connected");
    } catch (e) {
      this.logger.warn(
        `Kafka unavailable (${e.message}) — running without Kafka. ` +
          "Install via: brew install kafka && brew services start zookeeper && brew services start kafka",
      );
    }
  }

  async publish(
    topic: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    if (!this.connected) {
      this.logger.warn(
        `[Kafka] Skipping publish to [${topic}] — not connected`,
      );
      return;
    }
    await this.producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [
        {
          key: String(payload.orderId ?? payload.userId ?? Date.now()),
          value: JSON.stringify(payload),
          headers: {
            source: "order-service",
            timestamp: new Date().toISOString(),
          },
        },
      ],
    });
    this.logger.debug(`Published to [${topic}]: ${JSON.stringify(payload)}`);
  }

  async onModuleDestroy() {
    if (this.connected) await this.producer.disconnect();
  }
}
