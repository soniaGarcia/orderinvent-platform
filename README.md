# 🚀 Order & Inventory Management System (OrderInvent)
## Guía de Despliegue y Prueba End-to-End

Este repositorio contiene la orquestación local para levantar el ecosistema de microservicios mediante Docker Compose[cite: 14].

La solución implementa una arquitectura híbrida: comunicación síncrona vía **REST con resiliencia** (*Resilience4j*) para la interacción entre pedidos e inventario, y comunicación asíncrona mediante **Event-Driven Architecture (EDA)** con Apache Kafka para el procesamiento de notificaciones e idempotencia en auditoría[cite: 5, 8, 13].

---

## 🏛️ Visión General de Microservicios, Puertos y Persistencia

Cada microservicio cumple estrictamente con el patrón **Database per Service**, garantizando la autonomía de los datos y el escalado independiente:

| Microservicio | Puerto HTTP | Base de Datos | Responsabilidad Principal |
| :--- | :--- | :--- | :--- |
| **`order-service`** | `:8080` | `order_db` | Gestión del ciclo de vida de pedidos, orquestación síncrona y emisión de eventos de dominio a Kafka[cite: 8, 14]. |
| **`inventory-service`** | `:8081` | `inventory_db` | Control de stock y ejecución atómica batch de descuentos (*All-or-Nothing*)[cite: 14]. |
| **`notification-service`** | `:8082` | `notification_db` | Consumo de eventos de Kafka[cite: 8], simulación de notificaciones (Email/SMS)[cite: 13], control de idempotencia[cite: 13] y API REST de auditoría[cite: 5]. |

---

## 📩 Microservicio de Notificaciones e Idempotencia

El `notification-service` opera como componente consumidor y de auditoría dentro de la arquitectura orientada a eventos[cite: 5, 8]:

* **Consumidor de Kafka:** Escucha continuamente el tópico `order-events` bajo el grupo de consumidores `notification-group`[cite: 8].
* **Patrón de Idempotencia:** Para prevenir notificaciones duplicadas ante entregas repetidas en el bus de mensajes, el servicio verifica en la tabla `notification_logs` si la combinación de `orderId` y `orderStatus` ya fue procesada con éxito (`SENT`)[cite: 10, 12, 13].
* **Manejo de Duplicados:** Si se detecta un mensaje duplicado, se persiste el registro con estado `SKIPPED_DUPLICATE` y se omite el reenvío de la notificación[cite: 11, 13].
* **Endpoint REST de Auditoría:** Expone una interfaz de consulta para verificar el historial de notificaciones y el estado de entrega de cualquier pedido (`GET /api/v1/notifications/order/{orderId}`)[cite: 5].

---

## 📋 Prerrequisitos
* Docker Desktop instalado y en ejecución[cite: 14].
* Git[cite: 14].
* Cliente HTTP (cURL, Postman o navegador web)[cite: 14].

---

## 📁 Paso 1: Clonar los Repositorios en la Misma Carpeta

Para asegurar que las rutas relativas de construcción funcionen correctamente, la estructura de carpetas local debe ser la siguiente[cite: 14]:

```text
/tu-carpeta-de-trabajo/
├── orderinvent-platform/               # Este repositorio (Orquestación Docker)
├── orderinvent-order-service/          # Microservicio de Pedidos (Spring Boot)
├── orderinvent-inventory-service/      # Microservicio de Inventario (Spring Boot)
└── orderinvent-notification-service/   # Microservicio Consumidor (Spring Boot)
```

---

## ⚙️ Paso 2: Levantar el Ecosistema Completo

Abre tu terminal dentro de la carpeta `orderinvent-platform` y ejecuta[cite: 14]:

```bash
docker compose up --build -d
```

Este comando compilará las imágenes de cada microservicio usando los `Dockerfile` Multi-Stage de cada repositorio y levantará la infraestructura de Apache Kafka[cite: 14].

Verifica que todos los contenedores estén corriendo con[cite: 14]:
```bash
docker compose ps
```

---

## 🧪 Paso 3: Colección de Pruebas End-to-End (cURL)

### 1. Inicializar Stock en Inventory Service (Puerto 8081)[cite: 14]
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

### 2. Flujo Exitoso: Crear Pedido con Stock Suficiente (Puerto 8080)[cite: 14]
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
*Respuesta esperada:* HTTP 201 Created con estado `CONFIRMADO`[cite: 14].

### 3. Flujo Rechazado: Crear Pedido sobrepasando el Stock[cite: 14]
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
*Respuesta esperada:* HTTP 400 Bad Request con estado `RECHAZADO`[cite: 14].

### 4. Prueba de Resiliencia (Circuit Breaker y Fallback)[cite: 14]
Apaga intencionalmente el servicio de inventario para simular una caída de red[cite: 14]:

```bash
docker compose stop inventory-service
```

Envía una nueva orden hacia `order-service`[cite: 14]:
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
*Respuesta esperada:* HTTP 202 Accepted con estado `PENDING`[cite: 14]. El Circuit Breaker captura el fallo y envía el evento a Kafka[cite: 14].

Verifica la recepción del evento en `notification-service`[cite: 14]:
```bash
docker compose logs -f notification-service
```

### 5. Consultar Auditoría e Historial de Notificaciones (Puerto 8082)[cite: 5]
Una vez procesado el pedido, puedes consultar la auditoría de notificaciones registradas para dicho ID de orden[cite: 5]:

```bash
curl -X GET http://localhost:8082/api/v1/notifications/order/ORD-109283
```

---

## 🔗 Enlaces Útiles de la Aplicación Local[cite: 14]
* **Swagger UI (Order Service):** `http://localhost:8080/swagger-ui/index.html`[cite: 14]
* **Swagger UI (Inventory Service):** `http://localhost:8081/swagger-ui/index.html`[cite: 14]
* **Swagger UI (Notification Service):** `http://localhost:8082/swagger-ui/index.html`[cite: 14]
* **Kafdrop UI (Inspección de Kafka):** `http://localhost:9000`[cite: 14]
* **Health Check (Actuator Order Service):** `http://localhost:8080/actuator/health`[cite: 14]
* **Health Check (Actuator Inventory Service):** `http://localhost:8081/actuator/health`[cite: 14]
* **Health Check (Actuator Notification Service):** `http://localhost:8082/actuator/health`[cite: 14]