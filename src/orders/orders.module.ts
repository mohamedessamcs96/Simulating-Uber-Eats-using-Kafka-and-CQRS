import { Module } from "@nestjs/common";
import { CqrsModule } from "@nestjs/cqrs";
import { TypeOrmModule } from "@nestjs/typeorm";
import { OrdersController } from "./orders.controller";
import { BlockingOrdersController } from "./orders-blocking.controller";
import { SimulatorController } from "./simulator.controller";
import { PlaceOrderHandler } from "./commands/place-order.handler";
import { GetOrderStatusHandler } from "./queries/get-order-status.handler";
import { Order } from "./entities/order.entity";
import { OrderReadModel } from "./read-models/order.read-model";
import { KafkaProducerService } from "../kafka/kafka-producer.service";
import { KafkaConsumerService } from "../kafka/kafka-consumer.service";

@Module({
  imports: [CqrsModule, TypeOrmModule.forFeature([Order, OrderReadModel])],
  controllers: [
    OrdersController,
    BlockingOrdersController,
    SimulatorController,
  ],
  providers: [
    PlaceOrderHandler,
    GetOrderStatusHandler,
    KafkaProducerService,
    KafkaConsumerService,
  ],
})
export class OrdersModule {}
