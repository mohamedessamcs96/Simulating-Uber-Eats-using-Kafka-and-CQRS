import { CommandHandler, ICommandHandler, EventBus } from "@nestjs/cqrs";
import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { PlaceOrderCommand } from "./place-order.command";
import { OrderPlacedEvent } from "../events/order-placed.event";
import { Order, OrderStatus } from "../entities/order.entity";
import {
  OrderReadModel,
  OrderReadStatus,
} from "../read-models/order.read-model";
import { KafkaProducerService } from "../../kafka/kafka-producer.service";

/**
 * CQRS COMMAND HANDLER
 *
 * Handles the PlaceOrderCommand:
 *  1. Persists order to the write DB
 *  2. Publishes OrderPlacedEvent to the in-process EventBus
 *  3. Publishes to Kafka so other microservices react asynchronously
 *
 * BENEFIT: The HTTP handler returns immediately after emitting the command.
 * The restaurant, payment, and dispatch services react independently
 * via Kafka — no chain of blocking HTTP calls.
 */
@CommandHandler(PlaceOrderCommand)
export class PlaceOrderHandler implements ICommandHandler<PlaceOrderCommand> {
  constructor(
    @InjectRepository(Order)
    private readonly orderRepo: Repository<Order>,
    @InjectRepository(OrderReadModel)
    private readonly readModelRepo: Repository<OrderReadModel>,
    private readonly eventBus: EventBus,
    private readonly kafka: KafkaProducerService,
  ) {}

  async execute(command: PlaceOrderCommand): Promise<string> {
    const { userId, restaurantId, items, deliveryAddress } = command;

    // 1. Calculate total
    const totalAmount = items.reduce(
      (sum, item) => sum + item.price * item.quantity,
      0,
    );

    // 2. Persist to write-side DB (Command side)
    const order = this.orderRepo.create({
      userId,
      restaurantId,
      items,
      deliveryAddress,
      totalAmount,
      status: OrderStatus.PENDING,
    });
    const saved = await this.orderRepo.save(order);

    // 2b. Seed the read model with PENDING status (normally done by Kafka consumer)
    await this.readModelRepo.save(
      this.readModelRepo.create({
        orderId: saved.id,
        status: OrderReadStatus.PENDING,
      }),
    );

    // 3. Publish in-process domain event (for local CQRS projections)
    this.eventBus.publish(
      new OrderPlacedEvent(
        saved.id,
        userId,
        restaurantId,
        totalAmount,
        new Date(),
      ),
    );

    // 4. Publish to Kafka — fires-and-forgets to all downstream consumers:
    //    • payment-service   → charges the user
    //    • restaurant-service → notifies the restaurant
    //    • dispatch-service   → assigns a driver
    await this.kafka.publish("orders.placed", {
      orderId: saved.id,
      userId,
      restaurantId,
      items,
      totalAmount,
      deliveryAddress,
      timestamp: new Date().toISOString(),
    });

    return saved.id; // Returns immediately — no waiting for downstream!
  }
}
