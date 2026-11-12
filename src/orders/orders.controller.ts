import {
  Controller,
  Post,
  Get,
  Body,
  Param,
  HttpCode,
  HttpStatus,
} from "@nestjs/common";
import { CommandBus, QueryBus } from "@nestjs/cqrs";
import { PlaceOrderCommand } from "./commands/place-order.command";
import { GetOrderStatusQuery } from "./queries/get-order-status.query";

class PlaceOrderDto {
  restaurantId: string;
  items: Array<{ itemId: string; quantity: number; price: number }>;
  deliveryAddress: string;
}

/**
 * CQRS CONTROLLER
 *
 * ✅ POST /orders → dispatches a Command, returns immediately (202 Accepted)
 *    The client gets an orderId back in ~5ms. Payment, restaurant notification,
 *    and driver dispatch happen asynchronously via Kafka.
 *
 * ✅ GET /orders/:id → dispatches a Query, hits the read-model directly.
 *    The read-model is a denormalized view kept up-to-date by Kafka projectors.
 *
 * The controller knows NOTHING about downstream services. It just fires
 * commands into the bus and returns. No HTTP calls to payment-service here.
 */
@Controller("orders")
export class OrdersController {
  constructor(
    private readonly commandBus: CommandBus,
    private readonly queryBus: QueryBus,
  ) {}

  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  async placeOrder(@Body() dto: PlaceOrderDto) {
    const userId = "user-123";

    const orderId = await this.commandBus.execute(
      new PlaceOrderCommand(
        userId,
        dto.restaurantId,
        dto.items,
        dto.deliveryAddress,
      ),
    );

    return {
      success: true,
      data: {
        orderId,
        status: "PENDING",
        message: "Order received. Track progress at GET /orders/" + orderId,
      },
    };
  }

  @Get(":id")
  async getOrderStatus(@Param("id") orderId: string) {
    const order = await this.queryBus.execute(new GetOrderStatusQuery(orderId));
    return {
      success: true,
      data: order,
    };
  }
}
