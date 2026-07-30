# 📑 Especificación de Contratos de API REST (OpenAPI Standard)

Este documento define la especificación de los contratos de API REST expuestos por los microservicios del sistema **OrderInvent**, detallando sus endpoints, modelos de datos (*DTOs*) y códigos de respuesta HTTP.

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
  * `400 Bad Request`: Datos de solicitud inválidos o estructurados incorrectamente.

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
  * `200 OK`: Retorna `true` si el stock fue evaluado y descontado para **todos** los productos de la lista.
  * `200 OK`: Retorna `false` si al menos un producto no cuenta con existencias suficientes (*Rollback* completo de la transacción).

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
  * `201 Created` - **Flujo Exitoso (Estado `CONFIRMED`):**
    ```json
    {
      "orderId": "ORD-109283",
      "customerId": "CLI-1020",
      "status": "CONFIRMED",
      "createdAt": "2026-03-30T10:15:30Z"
    }
    ```
  * `201 Created` - **Flujo Rechazado por Stock (Estado `REJECTED`):**
    ```json
    {
      "orderId": "ORD-109284",
      "customerId": "CLI-1020",
      "status": "REJECTED",
      "rejectReason": "Stock insuficiente para uno o más productos solicitados."
    }
    ```
  * `202 Accepted` - **Flujo de Resiliencia / Fallback (Estado `PENDING`):**
    ```json
    {
      "orderId": "ORD-109285",
      "customerId": "CLI-1020",
      "status": "PENDING",
      "rejectReason": "Servicio de inventario no disponible. Orden encolada en Kafka para procesamiento asíncrono."
    }
    ```