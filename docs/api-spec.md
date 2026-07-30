# ?? Especificaci¨®n de Contratos de API REST y Eventos Async (OpenAPI & AsyncAPI)

Este documento define la especificaci¨®n de los contratos de comunicaci¨®n (s¨ªncronos v¨ªa REST y as¨ªncronos v¨ªa Kafka) expuestos y consumidos por los microservicios del sistema **OrderInvent**.

---

## 1. Inventory Service API (`:8081`)

### Base Path: `/api/v1/inventory`

#### 1.1. Registrar / Cargar Stock Inicial
* **M¨¦todo HTTP:** `POST /`
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
  * `400 Bad Request`: Datos de solicitud inv¨¢lidos.

#### 1.2. Descuento At¨®mico en Lote (All-or-Nothing)
* **M¨¦todo HTTP:** `POST /deduct`
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

#### 2.1. Crear Pedido Multi-¨ªtem
* **M¨¦todo HTTP:** `POST /`
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
      "rejectReason": "Stock insuficiente para uno o m¨¢s productos solicitados."
    }
    ```
  * `202 Accepted` - **Estado `PENDING` (Fallback Circuit Breaker):**
    ```json
    {
      "orderId": "ORD-109285",
      "customerId": "CLI-1020",
      "status": "PENDING",
      "rejectReason": "Servicio de inventario no disponible. Orden encolada para procesamiento as¨ªncrono."
    }
    ```

---

## 3. Notification Service API (`:8082`)

### Base Path: `/api/v1/notifications`

#### 3.1. Consultar Historial de Notificaciones y Auditor¨ªa por ID de Pedido
* **M¨¦todo HTTP:** `GET /order/{orderId}`
* **Path Variables:** `orderId` (String, ej: `"ORD-109283"`)
* **Respuestas:**
  * `200 OK`: Devuelve el listado de logs de auditor¨ªa para el pedido indicado.
    ```json
    [
      {
        "id": 1,
        "orderId": "ORD-109283",
        "orderStatus": "CONFIRMED",
        "channel": "EMAIL",
        "status": "SENT",
        "messageContent": "Pedido confirmado con ¨¦xito.",
        "createdAt": "2026-03-30T10:15:31Z"
      },
      {
        "id": 2,
        "orderId": "ORD-109283",
        "orderStatus": "CONFIRMED",
        "channel": "EMAIL",
        "status": "SKIPPED_DUPLICATE",
        "messageContent": "Evento duplicado ignorado.",
        "createdAt": "2026-03-30T10:15:32Z"
      }
    ]
  ```

---

## 4. Contratos de Eventos As¨ªncronos (Apache Kafka)

### T¨®pico: `order-events` (Consumer Group: `notification-group`)

Esquema de payload JSON publicado por `order-service` y consumido por `notification-service`:

* **Estructura del Mensaje (`OrderEventPayload`):**
  ```json
  {
    "orderId": "ORD-109283",
    "status": "CONFIRMED",
    "message": "Su pedido ha sido procesado exitosamente."
  }
  ```
* **Posibles Valores de `status`:** `CONFIRMED`, `REJECTED`, `PENDING`.
* **Garant¨ªas de Entrega e Idempotencia:** `notification-service` eval¨²a la combinaci¨®n de (`orderId`, `orderStatus`, `status=SENT`). Si el evento ya fue despachado previamente, registra el intento como `SKIPPED_DUPLICATE` en la tabla `notification_logs` sin reenviar el correo/SMS.