export class OrderPlacedEvent {
  constructor(
    public readonly orderId: string,
    public readonly userId: string,
    public readonly restaurantId: string,
    public readonly totalAmount: number,
    public readonly createdAt: Date,
  ) {}
}
