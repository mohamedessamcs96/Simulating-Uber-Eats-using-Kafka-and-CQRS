import {
  Controller,
  Post,
  Param,
  Body,
  HttpCode,
  HttpStatus,
  NotFoundException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import {
  OrderReadModel,
  OrderReadStatus,
} from "./read-models/order.read-model";

/**
 * KAFKA EVENTS SIMULATOR
 *
 * Simulates the Kafka consumer events that would normally be fired
 * by payment-service, restaurant-service, and dispatch-service.
 *
 * Without real Kafka running, use these endpoints to walk an order
 * through its full lifecycle exactly as the HTML frontend shows.
 *
 * Flow:
 *  POST /orders  →  (PENDING)
 *  POST /simulator/:id/payment-completed  →  (PAYMENT_CONFIRMED)
 *  POST /simulator/:id/restaurant-confirmed  →  (PREPARING)
 *  POST /simulator/:id/dispatch-assigned  →  (OUT_FOR_DELIVERY)
 *  POST /simulator/:id/delivered  →  (DELIVERED)
 *
 *  -- OR failure path --
 *  POST /simulator/:id/payment-failed  →  (CANCELLED)
 */
@Controller("simulator")
export class SimulatorController {
  constructor(
    @InjectRepository(OrderReadModel)
    private readonly readModelRepo: Repository<OrderReadModel>,
  ) {}

  private async findOrFail(orderId: string): Promise<OrderReadModel> {
    const order = await this.readModelRepo.findOne({ where: { orderId } });
    if (!order)
      throw new NotFoundException(`Order ${orderId} not found in read model`);
    return order;
  }

  /**
   * Simulates: payment-service consumed orders.placed → charged card
   * Kafka topic: payments.completed
   */
  @Post(":orderId/payment-completed")
  @HttpCode(HttpStatus.OK)
  async paymentCompleted(
    @Param("orderId") orderId: string,
    @Body() body: { paymentId?: string },
  ) {
    await this.findOrFail(orderId);
    const paymentId =
      body.paymentId ?? `pay_${Math.random().toString(36).slice(2, 10)}`;
    await this.readModelRepo.update(
      { orderId },
      { status: OrderReadStatus.PAYMENT_CONFIRMED, paymentId },
    );
    const updated = await this.readModelRepo.findOne({ where: { orderId } });
    return {
      success: true,
      event: "payments.completed",
      description: "payment-service charged the card — read model updated",
      data: updated,
    };
  }

  /**
   * Simulates: payment-service → payment failed
   * Kafka topic: payments.failed
   */
  @Post(":orderId/payment-failed")
  @HttpCode(HttpStatus.OK)
  async paymentFailed(
    @Param("orderId") orderId: string,
    @Body() body: { reason?: string },
  ) {
    await this.findOrFail(orderId);
    const reason = body.reason ?? "Insufficient funds";
    await this.readModelRepo.update(
      { orderId },
      { status: OrderReadStatus.CANCELLED, failureReason: reason },
    );
    const updated = await this.readModelRepo.findOne({ where: { orderId } });
    return {
      success: true,
      event: "payments.failed",
      description: "payment-service reported failure — order cancelled",
      data: updated,
    };
  }

  /**
   * Simulates: restaurant-service consumed orders.placed → confirmed the order
   * Kafka topic: restaurants.confirmed
   */
  @Post(":orderId/restaurant-confirmed")
  @HttpCode(HttpStatus.OK)
  async restaurantConfirmed(
    @Param("orderId") orderId: string,
    @Body() body: { estimatedPickupTime?: string },
  ) {
    await this.findOrFail(orderId);
    const estimatedPickupTime = body.estimatedPickupTime ?? "25 minutes";
    await this.readModelRepo.update(
      { orderId },
      { status: OrderReadStatus.PREPARING, estimatedPickupTime },
    );
    const updated = await this.readModelRepo.findOne({ where: { orderId } });
    return {
      success: true,
      event: "restaurants.confirmed",
      description:
        "restaurant-service confirmed and started preparing — read model updated",
      data: updated,
    };
  }

  /**
   * Simulates: dispatch-service consumed payments.completed → assigned a driver
   * Kafka topic: dispatch.assigned
   */
  @Post(":orderId/dispatch-assigned")
  @HttpCode(HttpStatus.OK)
  async dispatchAssigned(
    @Param("orderId") orderId: string,
    @Body() body: { driverName?: string; driverPhone?: string },
  ) {
    await this.findOrFail(orderId);
    const driverName = body.driverName ?? "John Driver";
    const driverPhone = body.driverPhone ?? "+44 7700 900123";
    await this.readModelRepo.update(
      { orderId },
      { status: OrderReadStatus.OUT_FOR_DELIVERY, driverName, driverPhone },
    );
    const updated = await this.readModelRepo.findOne({ where: { orderId } });
    return {
      success: true,
      event: "dispatch.assigned",
      description: "dispatch-service assigned a driver — read model updated",
      data: updated,
    };
  }

  /**
   * Simulates: dispatch-service → order delivered
   * Kafka topic: dispatch.delivered
   */
  @Post(":orderId/delivered")
  @HttpCode(HttpStatus.OK)
  async delivered(@Param("orderId") orderId: string) {
    await this.findOrFail(orderId);
    await this.readModelRepo.update(
      { orderId },
      { status: OrderReadStatus.DELIVERED, deliveredAt: new Date() },
    );
    const updated = await this.readModelRepo.findOne({ where: { orderId } });
    return {
      success: true,
      event: "dispatch.delivered",
      description: "dispatch-service confirmed delivery — order complete",
      data: updated,
    };
  }
}
