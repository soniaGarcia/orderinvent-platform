# Guía de Despliegue y Prueba End-to-End

Este repositorio contiene la orquestación local para levantar el ecosistema de microservicios mediante Docker Compose.

## Prerrequisitos
* Docker Desktop instalado y en ejecución.
* Git.
* Client HTTP (cURL, Postman o el navegador web).

---

## Paso 1: Clonar los Repositorios en la Misma Carpeta

Para asegurar que las rutas relativas de construcción funcionen correctamente, la estructura de carpetas local debe ser la siguiente:

```text
/tu-carpeta-de-trabajo/
├── orderinvent-platform/
├── orderinvent-order-service/
├── orderinvent-inventory-service/
└── orderinvent-notification-service/
```

---

## Paso 2: Levantar el Ecosistema Completo

Abre tu terminal dentro de la carpeta `orderinvent-platform` y ejecuta:

```bash
docker compose up --build -d
```

Este comando compilará las imágenes de cada microservicio usando los `Dockerfile` Multi-Stage de cada repositorio y levantará la infraestructura de Apache Kafka.

Verifica que todos los contenedores estén corriendo con:
```bash
docker compose ps
```

---

## Paso 3: Colección de Pruebas End-to-End (cURL)

### 1. Inicializar Stock en Inventory Service (Puerto 8081)
```bash
curl -X POST http://localhost:8081/api/v1/inventory \
  -H "Content-Type: application/json" \
  -d '{
    "productId": "1",
    "stock": 50
  }'
```

### 2. Flujo Exitoso: Crear Pedido con Stock Suficiente (Puerto 8080)
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerEmail": "cliente@logistics.com",
    "items": [
      { "productId": "1", "quantity": 2 }
    ]
  }'
```
*Respuesta esperada:* HTTP 201 Created con estado `CONFIRMADO`.

### 3. Flujo Rechazado: Crear Pedido sobrepasando el Stock
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerEmail": "cliente@logistics.com",
    "items": [
      { "productId": "1", "quantity": 999 }
    ]
  }'
```
*Respuesta esperada:* HTTP 400 Bad Request con estado `RECHAZADO`.

### 4. Prueba de Resiliencia (Circuit Breaker y Fallback)
Apaga intencionalmente el servicio de inventario para simular una caída de red:

```bash
docker compose stop inventory-service
```

Envía una nueva orden hacia `order-service`:
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerEmail": "cliente@logistics.com",
    "items": [
      { "productId": "1", "quantity": 1 }
    ]
  }'
```
*Respuesta esperada:* HTTP 202 Accepted con estado `PENDING`. El Circuit Breaker captura el fallo y envía el evento a Kafka.

Verifica la recepción del evento en `notification-service`:
```bash
docker compose logs -f notification-service
```

---

## Enlaces Utiles de la Aplicación Local
* **Swagger UI (Order Service):** `http://localhost:8080/swagger-ui/index.html`
* **Kafdrop UI (Inspección de Kafka):** `http://localhost:9000`
* **Health Check (Actuator):** `http://localhost:8080/actuator/health`