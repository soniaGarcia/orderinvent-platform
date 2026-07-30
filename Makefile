.PHONY: help up down restart logs ps clean seed test-flow test-circuit-breaker

help:
	@echo "======================================================================="
	@echo " 🚀 ORDERINVENT PLATFORM - COMMAND CENTER"
	@echo "======================================================================="
	@echo "  make up                  - Levanta todos los servicios en segundo plano"
	@echo "  make down                - Detiene y remueve contenedores y redes"
	@echo "  make build               - Reconstruye imágenes y levanta la plataforma"
	@echo "  make logs                - Muestra logs en tiempo real"
	@echo "  make ps                  - Estado actual de los contenedores"
	@echo "  make seed                - Carga datos/stock iniciales en Inventory Service"
	@echo "  make test-flow           - Ejecuta un pedido exitoso End-to-End"
	@echo "  make test-circuit-breaker- Simula caída de inventario y prueba resiliencia"
	@echo "  make clean               - Elimina volúmenes e imágenes huérfanas"
	@echo "======================================================================="

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose up --build -d

logs:
	docker compose logs -f

ps:
	docker compose ps

seed:
	@echo "📦 Cargando stock inicial en Inventory Service..."
	curl -s -X POST http://localhost:8081/api/v1/inventory \
		-H "Content-Type: application/json" \
		-d '{"productCode": "PROD-A100", "stock": 50}'
	@echo "\n"
	curl -s -X POST http://localhost:8081/api/v1/inventory \
		-H "Content-Type: application/json" \
		-d '{"productCode": "PROD-B200", "stock": 30}'
	@echo "\n✅ Stock inicial cargado exitosamente."

test-flow:
	@echo "🛒 Enviando orden de compra (Flujo Exitoso)..."
	curl -s -X POST http://localhost:8080/api/v1/orders \
		-H "Content-Type: application/json" \
		-d '{"customerId": "CLI-1020", "items": [{"productCode": "PROD-A100", "quantity": 2}, {"productCode": "PROD-B200", "quantity": 1}]}'
	@echo "\n✅ Solicitud procesada."

test-circuit-breaker:
	@echo "⚠️ Apagando inventory-service para simular falla..."
	docker compose stop inventory-service
	@echo "🛒 Enviando orden hacia order-service con circuito abierto..."
	curl -s -X POST http://localhost:8080/api/v1/orders \
		-H "Content-Type: application/json" \
		-d '{"customerId": "CLI-1020", "items": [{"productCode": "PROD-A100", "quantity": 1}]}'
	@echo "\n✅ Orden guardada como PENDING via Resilience4j."
	@echo "🔄 Reiniciando inventory-service..."
	docker compose start inventory-service

clean:
	docker compose down -v --remove-orphans