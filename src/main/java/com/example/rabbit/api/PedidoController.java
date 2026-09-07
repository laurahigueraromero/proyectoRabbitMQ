package com.example.rabbit.api;

import com.example.rabbit.domain.PedidoCreado;
import com.example.rabbit.producer.PedidoEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/pedidos")
public class PedidoController {

    private final PedidoEventPublisher publisher;

    public PedidoController(PedidoEventPublisher publisher) {
        this.publisher = publisher;
    }

    /**
     * En un sistema real aqui primero se guardaria el pedido en BD y LUEGO se publicaria
     * el evento (idealmente con patron outbox). Para practicar mensajeria nos saltamos la BD
     * y publicamos directamente.
     *
     * Devuelve 202 Accepted: "he aceptado la peticion, el procesamiento es asincrono".
     */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> crear(@RequestBody CrearPedidoRequest request) {
        String pedidoId = (request.pedidoId() == null || request.pedidoId().isBlank())
                ? UUID.randomUUID().toString()
                : request.pedidoId();

        // Si el cliente manda un eventId, lo respetamos. Asi puedes enviar DOS veces
        // el mismo evento y comprobar que el consumidor solo lo procesa una (idempotencia).
        String eventId = (request.eventId() == null || request.eventId().isBlank())
                ? UUID.randomUUID().toString()
                : request.eventId();

        PedidoCreado evento = new PedidoCreado(eventId, pedidoId, request.cliente(), request.total(), Instant.now());
        publisher.publicar(evento);

        return Map.of(
                "status", "accepted",
                "eventId", evento.eventId(),
                "pedidoId", evento.pedidoId()
        );
    }

    public record CrearPedidoRequest(String eventId, String pedidoId, String cliente, BigDecimal total) {}
}
