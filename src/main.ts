import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  await app.listen(3000);
  console.log('\n🍕 UberEats demo running → http://localhost:3000');
  console.log('   POST /orders          → CQRS + Kafka (async, 202)');
  console.log('   GET  /orders/:id      → read-model query');
  console.log('   POST /orders-blocking → traditional sync (201)\n');
}
bootstrap();
