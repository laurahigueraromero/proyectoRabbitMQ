package com.example.rabbit.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Evento de dominio. Es un DTO inmutable que viaja serializado a JSON por RabbitMQ.
 *
 * - eventId: identificador unico del EVENTO (no del pedido). Nos servira mas adelante
 *   para idempotencia: el consumidor lo usara para saber si ya proceso este mensaje.
 * - ocurridoEn: cuando ocurrio el hecho de negocio, no cuando se envio el mensaje.
 */
public record PedidoCreado(
        String eventId,
        String pedidoId,
        String cliente,
        BigDecimal total,
        Instant ocurridoEn
) {
    public static PedidoCreado nuevo(String pedidoId, String cliente, BigDecimal total) {
        return new PedidoCreado(
                UUID.randomUUID().toString(),
                pedidoId,
                cliente,
                total,
                Instant.now()
        );
    }
}
