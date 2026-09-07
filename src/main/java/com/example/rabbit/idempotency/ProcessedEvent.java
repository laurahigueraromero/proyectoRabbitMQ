package com.example.rabbit.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Fila de la tabla "processed_events". Guarda que un evento (por su eventId)
 * YA fue procesado, para no volver a procesarlo si el mensaje llega otra vez.
 *
 * La clave primaria es el propio eventId: la BD garantiza que no puede haber
 * dos filas con el mismo -> es la barrera real contra duplicados.
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", nullable = false, updatable = false, length = 64)
    private String eventId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
        // requerido por JPA
    }

    public ProcessedEvent(String eventId, Instant processedAt) {
        this.eventId = eventId;
        this.processedAt = processedAt;
    }

    public String getEventId() {
        return eventId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
