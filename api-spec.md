# 📑 Especificación de Contratos de API REST y Eventos Async (OpenAPI & AsyncAPI)

Este documento define la especificación de los contratos de comunicación (síncronos vía REST y asíncronos vía Kafka) expuestos y consumidos por los microservicios del sistema **OrderInvent**.

---

## 1. Inventory Service API (`:8081`)

### Base Path: `/api/v1/inventory`

#### 1.1. Registrar / Cargar Stock Inicial
* **Método HTTP:** `POST /`
* **Headers:** `Content-Type: application/json`
* **Request Body:**
  ```json
  {
    "productCode": "PROD-A100",
    "stock": 50
  }
  ```
* **Respuestas:**
  * `200 OK`: Stock cargado o actualizado exitosamente.
  * `400 Bad Request`: Datos de solicitud inválidos.

#### 1.2. Descuento Atómico en Lote (All-or-Nothing)
* **Método HTTP:** `POST /deduct`
* **Headers:** `Content-Type: application/json`
* **Request Body:**
  ```json
  [
    { "productCode": "PROD-A100", "quantity": 2 },
    { "productCode": "PROD-B200", "quantity": 1 }
  ]
  ```
* **Respuestas:**
  * `200 OK`: Retorna `true` si el stock fue descontado para **todos** los productos.
  * `200 OK`: Retorna `false` si al menos un producto no cuenta con existencias suficientes (*Rollback*).

---

## 2. Order Service API (`:8080`)

### Base Path: `/api/v1/orders`

#### 2.1. Crear Pedido Multi-Ítem
* **Método HTTP:** `POST /`
* **Headers:** `Content-Type: application/json`
* **Request Body:**
  ```json
  {
    "customerId": "CLI-1020",
    "items": [
      { "productCode": "PROD-A100", "quantity": 2 },
      { "productCode": "PROD-B200", "quantity": 1 }
    ]
  }
  ```
* **Respuestas:**
  * `201 Created` - **Estado `CONFIRMED`:**
    ```json
    {
      "orderId": "ORD-109283",
      "customerId": "CLI-1020",
      "status": "CONFIRMED",
      "createdAt": "2026-03-30T10:15:30Z"
    }
    ```
  * `201 Created` - **Estado `REJECTED`:**
    ```json
    {
      "orderId": "ORD-109284",
      "customerId": "CLI-1020",
      "status": "REJECTED",
      "rejectReason": "Stock insuficiente para uno o más productos solicitados."
    }
    ```
  * `202 Accepted` - **Estado `PENDING` (Fallback Circuit Breaker):**
    ```json
    {
      "orderId": "ORD-109285",
      "customerId": "CLI-1020",
      "status": "PENDING",
      "rejectReason": "Servicio de inventario no disponible. Orden encolada para procesamiento asíncrono."
    }
    ```

---

## 3. Notification Service API (`:8082`)

### Base Path: `/api/v1/notifications`

#### 3.1. Consultar Historial de Notificaciones y Auditoría por ID de Pedido
* **Método HTTP:** `GET /order/{orderId}`
* **Path Variables:** `orderId` (String, ej: `"ORD-109283"`)
* **Respuestas:**
  * `200 OK`: Devuelve el listado de logs de auditoría para el pedido indicado.
    ```json
    [
      {
        "id": 1,
        "orderId": "ORD-109283",
        "orderStatus": "CONFIRMADO",
        "channel": "EMAIL",
        "status": "SENT",
        "messageContent": "Pedido confirmado con éxito.",
        "createdAt": "2026-03-30T10:15:31Z"
      },
      {
        "id": 2,
        "orderId": "ORD-109283",
        "orderStatus": "CONFIRMADO",
        "channel": "EMAIL",
        "status": "SKIPPED_DUPLICATE",
        "messageContent": "Evento duplicado ignorado.",
        "createdAt": "2026-03-30T10:15:32Z"
      }
    ]
  ```

---

## 4. Contratos de Eventos Asíncronos (Apache Kafka)

### 4.1. Tópico: `order-events`
* **Publicador:** `order-service`
* **Consumidores:** `notification-service` (Grupo: `notification-group`), `inventory-service` (Grupo: `inventory-saga-group`).
* **Propósito:** Notificar cambios de estado en las órdenes para auditoría y disparar la reconciliación asíncrona de la Saga.

#### Esquema del Payload JSON:
```json
{
  "orderId": "ORD-109283",
  "status": "CONFIRMADO",
  "message": "Pedido confirmado y stock descontado.",
  "items": [
    { "productCode": "PROD-A100", "quantity": 2 },
    { "productCode": "PROD-B200", "quantity": 1 }
  ]
}
```
* **Valores posibles de `status`:** `PENDIENTE`, `CONFIRMADO`, `RECHAZADO`.
* **Garantías de Entrega e Idempotencia:** `notification-service` evalúa la combinación (`orderId`, `orderStatus`). Si el evento ya fue procesado, registra la traza como `SKIPPED_DUPLICATE` en la tabla `notification_logs` omitiendo el reenvío de avisos.

---

### 4.2. Tópico: `inventory-events`
* **Publicador:** `inventory-service`
* **Consumidor:** `order-service` (Grupo: `order-saga-group`)
* **Propósito:** Responder el resultado del procesamiento diferido de inventario durante la reconciliación de la Saga.

#### Esquema del Payload JSON:
```json
{
  "orderId": "ORD-109285",
  "status": "INVENTORY_SUCCESS",
  "message": "Stock descontado exitosamente en procesamiento diferido."
}
```
* **Valores posibles de `status`:** `INVENTORY_SUCCESS`, `INVENTORY_FAILED`.