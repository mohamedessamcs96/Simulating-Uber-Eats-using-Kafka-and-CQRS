import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
} from "@nestjs/common";
import { Kafka, Consumer, EachMessagePayload } from "kafkajs";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import {
  OrderReadModel,
  OrderReadStatus,
} from "../orders/read-models/order.read-model";

/**
 * KAFKA CONSUMER — Order Service (Read Model Projector)
 *
 * Subscribes to events published by OTHER microservices and
 * updates the order read-model accordingly.
 *
 * This is the "projection" side of CQRS — keeping the read
 * database eventually consistent with the system's overall state.
 *
 * Each consumer group processes messages independently, so
 * payment-service, restaurant-service, and dispatch-service
 * all get their own copy of every event.
 *
 * NOTE: If Kafka is not running the app still starts gracefully.
 */
@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private consumer: Consumer;

  private readonly kafka = new Kafka({
    clientId: "order-service-consumer",
    brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
    retry: { initialRetryTime: 300, retries: 2 },
  });

  constructor(
    @InjectRepository(OrderReadModel)
    private readonly readModelRepo: Repository<OrderReadModel>,
  ) {}

  async onModuleInit() {
    this.consumer = this.kafka.consumer({ groupId: "order-service-group" });
    try {
      await this.consumer.connect();

      await this.consumer.subscribe({
        topics: [
          "payments.completed", // from payment-service
          "payments.failed", // from payment-service
          "restaurants.confirmed", // from restaurant-service
          "dispatch.assigned", // from dispatch-service
          "dispatch.delivered", // from dispatch-service
        ],
        fromBeginning: false,
      });

      await this.consumer.run({
        eachMessage: async (payload: EachMessagePayload) => {
          await this.handleMessage(payload);
        },
      });

      this.logger.log("Kafka consumer running");
    } catch (e) {
      this.logger.warn(
        `Kafka unavailable (${e.message}) — consumer not started. ` +
          "Install via: brew install kafka && brew services start zookeeper && brew services start kafka",
      );
    }
  }

  private async handleMessage({ topic, message }: EachMessagePayload) {
    const data = JSON.parse(message.value?.toString() ?? "{}");
    this.logger.debug(`Received [${topic}]: ${JSON.stringify(data)}`);

    switch (topic) {
      case "payments.completed":
        await this.readModelRepo.update(
          { orderId: data.orderId },
          {
            status: OrderReadStatus.PAYMENT_CONFIRMED,
            paymentId: data.paymentId,
          },
        );
        break;

      case "payments.failed":
        await this.readModelRepo.update(
          { orderId: data.orderId },
          { status: OrderReadStatus.CANCELLED, failureReason: data.reason },
        );
        break;

      case "restaurants.confirmed":
        await this.readModelRepo.update(
          { orderId: data.orderId },
          {
            status: OrderReadStatus.PREPARING,
            estimatedPickupTime: data.estimatedPickupTime,
          },
        );
        break;

      case "dispatch.assigned":
        await this.readModelRepo.update(
          { orderId: data.orderId },
          {
            status: OrderReadStatus.OUT_FOR_DELIVERY,
            driverName: data.driverName,
            driverPhone: data.driverPhone,
          },
        );
        break;

      case "dispatch.delivered":
        await this.readModelRepo.update(
          { orderId: data.orderId },
          { status: OrderReadStatus.DELIVERED, deliveredAt: new Date() },
        );
        break;
    }
  }

  async onModuleDestroy() {
    if (this.consumer) {
      try {
        await this.consumer.disconnect();
      } catch {}
    }
  }
}
