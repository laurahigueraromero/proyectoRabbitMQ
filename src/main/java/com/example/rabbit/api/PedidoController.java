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

// Map ==> diccionario de clave valor. En este caso devolvemos un diccionario con el status, eventId y pedidoId (return)

    public Map<String, String> crear(@RequestBody CrearPedidoRequest request) {
        String pedidoId = (request.pedidoId() == null || request.pedidoId().isBlank())
            // si no hay pedidoId en el request, generamos uno nuevo
                ? UUID.randomUUID().toString()
                // si hay pedidoId en el request, lo respetamos
                : request.pedidoId();

       
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


 //  clase que representa el cuerpo JSON que llega en el POST. Un record es una forma corta de declarar una clase inmutable que solo lleva datos. le dice a Java: "crea una clase con estos 4 campos". Y el compilador genera automáticamente por ti:

// - un constructor con esos 4 parámetros,
// - un getter por campo (se llaman eventId(), pedidoId(), cliente(), total() — sin get),
// - equals(), hashCode() y toString(),
// - los campos son final: una vez creado, no se modifica.
    public record CrearPedidoRequest(String eventId, String pedidoId, String cliente, BigDecimal total) {}
}
