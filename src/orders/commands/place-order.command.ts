export class PlaceOrderCommand {
  constructor(
    public readonly userId: string,
    public readonly restaurantId: string,
    public readonly items: Array<{
      itemId: string;
      quantity: number;
      price: number;
    }>,
    public readonly deliveryAddress: string,
  ) {}
}
