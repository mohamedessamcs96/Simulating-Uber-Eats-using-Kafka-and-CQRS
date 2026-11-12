import {
  Controller,
  Post,
  Get,
  Body,
  Param,
  HttpCode,
  HttpStatus,
  Logger,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Order, OrderStatus } from "./entities/order.entity";

/** Simulates a real HTTP call to a downstream service taking `ms` milliseconds */
const simulateHttpCall = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

class PlaceOrderDto {
  restaurantId: string;
  items: Array<{ itemId: string; quantity: number; price: number }>;
  deliveryAddress: string;
}

/**
 * TRADITIONAL / BLOCKING CONTROLLER
 *
 * ❌ PROBLEM: Everything is sequential and synchronous.
 *
 * The HTTP request is HELD OPEN while we:
 *   1. Save the order to DB
 *   2. HTTP call → payment-service  (wait for response... ~200ms)
 *   3. HTTP call → restaurant-service (wait for response... ~150ms)
 *   4. HTTP call → dispatch-service  (wait for response... ~300ms)
 *
 * Total latency: ~650ms+ per request.
 *
 * If payment-service is slow or down:
 *   - Our endpoint also becomes slow / times out
 *   - Thread is blocked the entire time
 *   - Under load: connection pool exhaustion → 503s cascade
 *
 * All services are TIGHTLY COUPLED via direct HTTP.
 */
@Controller("orders-blocking")
export class BlockingOrdersController {
  private readonly logger = new Logger(BlockingOrdersController.name);

  constructor(
    @InjectRepository(Order)
    private readonly orderRepo: Repository<Order>,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  async placeOrder(@Body() dto: PlaceOrderDto) {
    const userId = "user-123";

    const totalAmount = dto.items.reduce(
      (sum, i) => sum + i.price * i.quantity,
      0,
    );

    const order = await this.orderRepo.save(
      this.orderRepo.create({
        userId,
        restaurantId: dto.restaurantId,
        items: dto.items,
        deliveryAddress: dto.deliveryAddress,
        totalAmount,
        status: OrderStatus.PENDING,
      }),
    );
    this.logger.log(`Order ${order.id} saved`);

    // ❌ BLOCKING call to payment-service — ~200ms wait (simulated)
    // In production: await firstValueFrom(httpService.post('http://payment-service:3001/charge', ...))
    this.logger.log(`[payment-service] POST /charge — waiting ~200ms...`);
    await simulateHttpCall(200);
    const paymentId = `pay_${Math.random().toString(36).slice(2, 10)}`;
    await this.orderRepo.update(order.id, {
      status: OrderStatus.PAYMENT_PROCESSING,
      paymentId,
    });
    this.logger.log(`[payment-service] charged — paymentId: ${paymentId}`);

    // ❌ BLOCKING call to restaurant-service — ~150ms wait (simulated)
    // In production: await firstValueFrom(httpService.post('http://restaurant-service:3002/notify', ...))
    this.logger.log(`[restaurant-service] POST /notify — waiting ~150ms...`);
    await simulateHttpCall(150);
    await this.orderRepo.update(order.id, {
      status: OrderStatus.RESTAURANT_NOTIFIED,
    });
    this.logger.log(`[restaurant-service] notified`);

    // ❌ BLOCKING call to dispatch-service — ~300ms wait (simulated)
    // In production: await firstValueFrom(httpService.post('http://dispatch-service:3003/assign', ...))
    this.logger.log(`[dispatch-service] POST /assign — waiting ~300ms...`);
    await simulateHttpCall(300);
    const driverName = "John Driver";
    await this.orderRepo.update(order.id, {
      status: OrderStatus.DRIVER_ASSIGNED,
      driverName,
    });
    this.logger.log(`[dispatch-service] driver assigned: ${driverName}`);

    const saved = await this.orderRepo.findOne({ where: { id: order.id } });
    return {
      success: true,
      data: saved,
    };
  }

  @Get(":id")
  async getOrderStatus(@Param("id") orderId: string) {
    const order = await this.orderRepo.findOne({ where: { id: orderId } });
    return {
      success: true,
      data: order ?? null,
    };
  }
}
