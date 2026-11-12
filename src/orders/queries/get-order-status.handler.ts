import { IQueryHandler, QueryHandler } from "@nestjs/cqrs";
import { InjectRepository } from "@nestjs/typeorm";
import { NotFoundException } from "@nestjs/common";
import { Repository } from "typeorm";
import { GetOrderStatusQuery } from "./get-order-status.query";
import { OrderReadModel } from "../read-models/order.read-model";

@QueryHandler(GetOrderStatusQuery)
export class GetOrderStatusHandler
  implements IQueryHandler<GetOrderStatusQuery>
{
  constructor(
    @InjectRepository(OrderReadModel)
    private readonly readModelRepo: Repository<OrderReadModel>,
  ) {}

  async execute(query: GetOrderStatusQuery): Promise<OrderReadModel> {
    const order = await this.readModelRepo.findOne({
      where: { orderId: query.orderId },
    });
    if (!order) {
      throw new NotFoundException(`Order ${query.orderId} not found`);
    }
    return order;
  }
}
