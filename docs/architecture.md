# 🏗️ Documentación de Arquitectura de Software

## 1. Diagrama de Contenedores (C4 Model)

```mermaid
C4Container
    title Diagrama de Contenedores - Ecosistema de Logística

    Person(customer, "Cliente", "Solicita la creación de pedidos")
    
    System_Boundary(b1, "Plataforma de Logística") {
        Container(order_svc, "Order Service", "Java 17 / Spring Boot 3", "Gestión de pedidos, estado local y Resilience4j")
        Container(inv_svc, "Inventory Service", "Java 17 / Spring Boot 3", "Verificación y descuento de stock")
        Container(notif_svc, "Notification Service", "Java 17 / Spring Boot 3", "Consumo asíncrono de eventos")
        
        ContainerDb(order_db, "Order DB", "PostgreSQL 15", "Base de Datos de Pedidos")
        ContainerDb(inv_db, "Inventory DB", "PostgreSQL 15", "Base de Datos de Inventario")
        
        ContainerQueue(kafka, "Message Broker", "Apache Kafka (KRaft)", "Event Streaming de Alta Velocidad")
    }

    Rel(customer, order_svc, "POST /api/v1/orders", "REST / JSON")
    Rel(order_svc, inv_svc, "POST /deduct (Circuit Breaker)", "REST / JSON")
    Rel(order_svc, order_db, "JDBC", "SQL")
    Rel(inv_svc, inv_db, "JDBC", "SQL")
    Rel(order_svc, kafka, "Publica 'order-events'", "Kafka Protocol")
    Rel(kafka, notif_svc, "Suscrito a 'order-events'", "Kafka Protocol")