# 🚀 Order & Inventory Management System (OrderInvent)
## Guía de Despliegue y Prueba End-to-End

Este repositorio contiene la orquestación local para levantar el ecosistema de microservicios mediante Docker Compose.

La solución implementa una arquitectura híbrida: comunicación síncrona vía **REST con resiliencia** (*Resilience4j*) para la interacción entre pedidos e inventario, y comunicación asíncrona mediante **Event-Driven Architecture (EDA)** con Apache Kafka para el procesamiento de notificaciones e idempotencia en auditoría.

---

## 🏛️ Visión General de Microservicios, Puertos y Persistencia

Cada microservicio cumple estrictamente con el patrón **Database per Service**, garantizando la autonomía de los datos y el escalado independiente:

| Microservicio | Puerto HTTP | Base de Datos | Responsabilidad Principal |
| :--- | :--- | :--- | :--- |
| **`order-service`** | `:8080` | `order_db` | Gestión del ciclo de vida de pedidos, orquestación síncrona y emisión de eventos de dominio a Kafka. |
| **`inventory-service`** | `:8081` | `inventory_db` | Control de stock y ejecución atómica batch de descuentos (*All-or-Nothing*). |
| **`notification-service`** | `:8082` | `notification_db` | Consumo de eventos de Kafka, simulación de notificaciones (Email/SMS), control de idempotencia y API REST de auditoría. |

---

## 📩 Microservicio de Notificaciones e Idempotencia

El `notification-service` opera como componente consumidor y de auditoría dentro de la arquitectura orientada a eventos:

* **Consumidor de Kafka:** Escucha continuamente el tópico `order-events` bajo el grupo de consumidores `notification-group`.
* **Patrón de Idempotencia:** Para prevenir notificaciones duplicadas ante entregas repetidas en el bus de mensajes, el servicio verifica en la tabla `notification_logs` si la combinación de `orderId` y `orderStatus` ya fue procesada con éxito (`SENT`).
* **Manejo de Duplicados:** Si se detecta un mensaje duplicado, se persiste el registro con estado `SKIPPED_DUPLICATE` y se omite el reenvío de la notificación.
* **Endpoint REST de Auditoría:** Expone una interfaz de consulta para verificar el historial de notificaciones y el estado de entrega de cualquier pedido (`GET /api/v1/notifications/order/{orderId}`).

---

## 📋 Prerrequisitos
* Docker Desktop instalado y en ejecución.
* Git.
* Cliente HTTP (cURL, Postman o navegador web).

---

## 📁 Paso 1: Clonar los Repositorios en la Misma Carpeta

Para asegurar que las rutas relativas de construcción funcionen correctamente, la estructura de carpetas local debe ser la siguiente:

```text
/tu-carpeta-de-trabajo/
├── orderinvent-platform/               # Este repositorio (Orquestación Docker)
├── orderinvent-order-service/          # Microservicio de Pedidos (Spring Boot)
├── orderinvent-inventory-service/      # Microservicio de Inventario (Spring Boot)
└── orderinvent-notification-service/   # Microservicio Consumidor (Spring Boot)
```

---

## ⚙️ Paso 2: Levantar el Ecosistema Completo

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

## 🧪 Paso 3: Colección de Pruebas End-to-End (cURL)

### 1. Inicializar Stock en Inventory Service (Puerto 8081)
```bash
# Cargar Producto A
curl -X POST http://localhost:8081/api/v1/inventory \
  -H "Content-Type: application/json" \
  -d '{
    "productCode": "PROD-A100",
    "stock": 50
  }'

# Cargar Producto B
curl -X POST http://localhost:8081/api/v1/inventory \
  -H "Content-Type: application/json" \
  -d '{
    "productCode": "PROD-B200",
    "stock": 30
  }'
```

### 2. Flujo Exitoso: Crear Pedido con Stock Suficiente (Puerto 8080)
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CLI-1020",
    "items": [
      { "productCode": "PROD-A100", "quantity": 2 },
      { "productCode": "PROD-B200", "quantity": 1 }
    ]
  }'
```
*Respuesta esperada:* HTTP 201 Created con estado `CONFIRMADO`.

### 3. Flujo Rechazado: Crear Pedido sobrepasando el Stock
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CLI-1020",
    "items": [
      { "productCode": "PROD-A100", "quantity": 1 },
      { "productCode": "PROD-B200", "quantity": 999 }
    ]
  }'
```
*Respuesta esperada:* HTTP 400 Bad Request con estado `RECHAZADO`.

### 4. Prueba de Resiliencia (Circuit Breaker y Fallback a PENDIENTE)
Apaga intencionalmente el servicio de inventario para simular una caída de red:

```bash
docker compose stop inventory-service
```

Envía una nueva orden hacia `order-service`:
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CLI-1020",
    "items": [
      { "productCode": "PROD-A100", "quantity": 1 }
    ]
  }'
```
*Respuesta esperada:* HTTP 202 Accepted con estado **`PENDIENTE`**. El Circuit Breaker captura el fallo, registra la orden en base de datos y publica el evento en Kafka.

---

### 5. Prueba de Reconciliación Asíncrona (Saga Coreografiada)
Con la orden anterior registrada en estado `PENDIENTE`, vuelve a encender el microservicio de inventario:

```bash
docker compose start inventory-service
```

Al iniciar, `inventory-service` consumirá automáticamente el evento `PENDIENTE` acumulado en Kafka, descontará el stock y enviará la confirmación hacia `order-service`.

#### Verificación del Cierre de la Saga:
1. **Consultar la orden en `order-service`:**
   ```bash
   curl -X GET http://localhost:8080/api/v1/orders/1
   ```
   *Respuesta esperada:* El estado habrá cambiado automáticamente de `PENDIENTE` a **`CONFIRMADO`**.

2. **Consultar el Historial de Auditoría de Notificaciones:**
   ```bash
   curl -X GET http://localhost:8082/api/v1/notifications/order/1
   ```
   *Respuesta esperada:* Verás **dos registros de auditoría** para el mismo pedido: el primero con estado `PENDIENTE` (emitido durante la caída) y el segundo con estado `CONFIRMADO` (emitido al completarse la Saga).

---

## 🔗 Enlaces Útiles de la Aplicación Local
* **Swagger UI (Order Service):** `http://localhost:8080/swagger-ui/index.html`
* **Swagger UI (Inventory Service):** `http://localhost:8081/swagger-ui/index.html`
* **Swagger UI (Notification Service):** `http://localhost:8082/swagger-ui/index.html`
* **Kafdrop UI (Inspección de Kafka):** `http://localhost:9000`
* **Health Check (Actuator Order Service):** `http://localhost:8080/actuator/health`
* **Health Check (Actuator Inventory Service):** `http://localhost:8081/actuator/health`
* **Health Check (Actuator Notification Service):** `http://localhost:8082/actuator/health`
