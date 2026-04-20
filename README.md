# UberEats Microservices Demo

A NestJS demo comparing **CQRS + Kafka async architecture** vs **traditional blocking/sync architecture** — side by side.

---

## Requirements

- [Node.js](https://nodejs.org) v18+
- npm (comes with Node)
- No Docker, no database server, no Kafka required to get started

---

## First-Time Setup

Run this once to install all dependencies:

```bash
cd "/Users/herotech/Downloads/Micro Services - Uber Eats"
npm install
```

---

## Running the Backend

```bash
cd "/Users/herotech/Downloads/Micro Services - Uber Eats"
KAFKAJS_NO_PARTITIONER_WARNING=1 npx ts-node -r tsconfig-paths/register src/main.ts
```

You should see:

```
🍕 UberEats demo running → http://localhost:3000
   POST /orders          → CQRS + Kafka (async, 202)
   GET  /orders/:id      → read-model query
   POST /orders-blocking → traditional sync (201)
```

> **Kafka warnings are expected** — the app runs fine without Kafka. Orders are saved to a local SQLite file (`ubereats-dev.db`).

---

## Stopping the Backend

Press `Ctrl + C` in the terminal running the server.

Or force-kill anything on port 3000:

```bash
kill $(lsof -ti :3000)
```

---

## Running the Frontend

The frontend is a standalone HTML file — no server needed:

```bash
open "/Users/herotech/Downloads/Micro Services - Uber Eats/ubereats-comparison-frontend.html"
```

Or just double-click `ubereats-comparison-frontend.html` in Finder.

Click **"Place Order"** to watch the Kafka+CQRS flow vs blocking flow animate side-by-side.

---

## API Endpoints

Base URL: `http://localhost:3000`

### CQRS + Kafka (Async) — recommended path

| Method | Path | Description |
|---|---|---|
| `POST` | `/orders` | Place an order — returns **202** in ~10ms |
| `GET` | `/orders/:id` | Get order status from the read model |

### Blocking / Traditional (Sync)

| Method | Path | Description |
|---|---|---|
| `POST` | `/orders-blocking` | Place an order — holds connection until all downstream services respond |
| `GET` | `/orders-blocking/:id` | Get order from the write table |

---

## Response Format

All endpoints return:

```json
{
  "success": true,
  "data": { ... }
}
```

### Example — POST /orders

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "restaurantId": "rest-001",
    "items": [
      { "itemId": "pizza", "quantity": 2, "price": 12.50 }
    ],
    "deliveryAddress": "123 Main St, London"
  }'
```

Response:

```json
{
  "success": true,
  "data": {
    "orderId": "cf19dd49-4ed9-4994-8379-a9d4c57a24fc",
    "status": "PENDING",
    "message": "Order received. Track progress at GET /orders/cf19dd49-..."
  }
}
```

### Example — GET /orders/:id

```bash
curl http://localhost:3000/orders/cf19dd49-4ed9-4994-8379-a9d4c57a24fc
```

Response:

```json
{
  "success": true,
  "data": {
    "id": "...",
    "orderId": "cf19dd49-...",
    "status": "PENDING",
    "paymentId": null,
    "driverName": null,
    "createdAt": "2026-04-20T10:00:00.000Z",
    "updatedAt": "2026-04-20T10:00:00.000Z"
  }
}
```

---

## Postman Collection

Import `UberEats-Demo.postman_collection.json` into Postman:

1. Open Postman
2. Click **Import**
3. Drag in `UberEats-Demo.postman_collection.json`

The collection has pre-written tests and automatically saves the `orderId` from Place Order so you can immediately run Get Order Status without copy-pasting.

---

## Project Structure

```
src/
├── main.ts                          # Entry point — starts server on port 3000
├── app.module.ts                    # Root module — TypeORM (SQLite) + OrdersModule
├── kafka/
│   ├── kafka-producer.service.ts   # Publishes events to Kafka topics
│   └── kafka-consumer.service.ts   # Subscribes to events, updates read model
└── orders/
    ├── orders.module.ts             # Wires everything together
    ├── orders.controller.ts         # CQRS endpoints (POST/GET /orders)
    ├── orders-blocking.controller.ts# Blocking endpoints (POST/GET /orders-blocking)
    ├── commands/
    │   ├── place-order.command.ts   # Command definition
    │   └── place-order.handler.ts  # Saves order + publishes Kafka event
    ├── queries/
    │   ├── get-order-status.query.ts
    │   └── get-order-status.handler.ts # Reads from the read model
    ├── events/
    │   └── order-placed.event.ts    # Domain event definition
    ├── entities/
    │   └── order.entity.ts          # Write-side DB table
    └── read-models/
        └── order.read-model.ts      # Read-side DB table (projected by Kafka consumer)
```

---

## Optional: Enable Kafka

To enable real Kafka event streaming:

```bash
# Install Kafka via Homebrew
brew install kafka

# Start ZooKeeper then Kafka
brew services start zookeeper
brew services start kafka

# Create the required topics
kafka-topics --create --topic orders.placed --bootstrap-server localhost:9092
kafka-topics --create --topic payments.completed --bootstrap-server localhost:9092
kafka-topics --create --topic payments.failed --bootstrap-server localhost:9092
kafka-topics --create --topic restaurants.confirmed --bootstrap-server localhost:9092
kafka-topics --create --topic dispatch.assigned --bootstrap-server localhost:9092
kafka-topics --create --topic dispatch.delivered --bootstrap-server localhost:9092
```

Then restart the backend — it will automatically connect.

---

## Reset the Database

Delete the SQLite file and restart the backend — it recreates the schema automatically:

```bash
rm "/Users/herotech/Downloads/Micro Services - Uber Eats/ubereats-dev.db"
```
# Simulating-Uber-Eats-using-Kafka-and-CQRS
