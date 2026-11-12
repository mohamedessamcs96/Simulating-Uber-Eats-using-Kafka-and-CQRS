import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";

export enum OrderReadStatus {
  PENDING = "PENDING",
  PAYMENT_CONFIRMED = "PAYMENT_CONFIRMED",
  CANCELLED = "CANCELLED",
  PREPARING = "PREPARING",
  OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY",
  DELIVERED = "DELIVERED",
}

@Entity("order_read_models")
export class OrderReadModel {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ unique: true })
  orderId: string;

  @Column({ type: "text", default: OrderReadStatus.PENDING })
  status: OrderReadStatus;

  @Column({ nullable: true })
  paymentId: string;

  @Column({ nullable: true })
  failureReason: string;

  @Column({ nullable: true })
  estimatedPickupTime: string;

  @Column({ nullable: true })
  driverName: string;

  @Column({ nullable: true })
  driverPhone: string;

  @Column({ nullable: true })
  deliveredAt: Date;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
