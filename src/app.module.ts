import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { OrdersModule } from "./orders/orders.module";
import { Order } from "./orders/entities/order.entity";
import { OrderReadModel } from "./orders/read-models/order.read-model";

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: "sqlite",
      database: "ubereats-dev.db",
      entities: [Order, OrderReadModel],
      synchronize: true,
      logging: false,
    }),
    OrdersModule,
  ],
})
export class AppModule {}
