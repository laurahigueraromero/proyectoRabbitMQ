package com.example.rabbit.idempotency;

import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Repositorio JPA. Nos da gratis existsById(eventId), saveAndFlush(...), etc.
 * El tipo de la clave primaria es String (el eventId).
 */
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
