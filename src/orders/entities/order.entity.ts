import { Entity, Column, PrimaryGeneratedColumn, CreateDateColumn } from 'typeorm';

export enum OrderStatus {
  PENDING = 'PENDING',
  PAYMENT_PROCESSING = 'PAYMENT_PROCESSING',
  RESTAURANT_NOTIFIED = 'RESTAURANT_NOTIFIED',
  DRIVER_ASSIGNED = 'DRIVER_ASSIGNED',
  CANCELLED = 'CANCELLED',
}

@Entity('orders')
export class Order {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column()
  userId: string;

  @Column()
  restaurantId: string;

  @Column({ type: 'simple-json' })
  items: Array<{ itemId: string; quantity: number; price: number }>;

  @Column()
  deliveryAddress: string;

  @Column({ type: 'float' })
  totalAmount: number;

  @Column({ type: 'text', default: OrderStatus.PENDING })
  status: OrderStatus;

  @Column({ nullable: true })
  paymentId: string;

  @Column({ nullable: true })
  driverName: string;

  @CreateDateColumn()
  createdAt: Date;
}
