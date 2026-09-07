package com.example.rabbit.consumer;

import com.example.rabbit.domain.PedidoCreado;
import com.example.rabbit.idempotency.ProcessedEvent;
import com.example.rabbit.idempotency.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class PedidoCreadoListener {

    private static final Logger log = LoggerFactory.getLogger(PedidoCreadoListener.class);

    private final ProcessedEventRepository processedEvents;

    public PedidoCreadoListener(ProcessedEventRepository processedEvents) {
        this.processedEvents = processedEvents;
    }

    /**
     * Flujo del consumidor:
     *   1. Deserializa JSON -> PedidoCreado (Jackson2JsonMessageConverter).
     *   2. Idempotencia: si el eventId ya esta en processed_events -> es un duplicado,
     *      se ignora y se hace ACK (no se repite la logica de negocio).
     *   3. Logica de negocio.
     *   4. Registra el eventId en processed_events.
     *
     * Todo bajo @Transactional (transaccion de BD):
     *   - Si la logica de negocio falla -> rollback -> el evento NO queda marcado
     *     como procesado -> los reintentos (y, si se agotan, la DLQ) siguen aplicando.
     *   - Si todo va bien -> se confirma la fila y el contenedor hace ACK del mensaje.
     *
     * Nota: el ACK a RabbitMQ ocurre despues de que el metodo retorne; no esta dentro
     * de la transaccion de BD. Si el proceso muere entre el commit y el ACK, el mensaje
     * se reentrega -> pero la fila en processed_events ya existe -> el paso 2 lo frena.
     * Justo por eso existe la tabla.
     */
    @RabbitListener(queues = "${app.messaging.queue.pedido-creado}")
    @Transactional
    public void onPedidoCreado(PedidoCreado evento,
                               @Header(AmqpHeaders.MESSAGE_ID) String messageId) {

        // 2. IDEMPOTENCIA
        if (processedEvents.existsById(evento.eventId())) {
            log.info("Evento DUPLICADO, se ignora. eventId={} pedidoId={}",
                    evento.eventId(), evento.pedidoId());
            return;
        }

        log.info("Procesando PedidoCreado eventId={} messageId={} pedidoId={} cliente={} total={}",
                evento.eventId(), messageId, evento.pedidoId(), evento.cliente(), evento.total());

        // --- Fallo simulado para ver reintentos + DLQ (cliente "boom") ---
        if ("boom".equalsIgnoreCase(evento.cliente())) {
            throw new IllegalStateException("Fallo simulado procesando el pedido de " + evento.cliente());
        }

        // 3. LOGICA DE NEGOCIO real (actualizar stock, enviar email, etc.)

        // 4. Marcar como procesado. saveAndFlush fuerza el INSERT ahora, para que un
        //    posible choque de clave primaria (dos consumidores a la vez) salte aqui.
        try {
            processedEvents.saveAndFlush(new ProcessedEvent(evento.eventId(), Instant.now()));
        } catch (DataIntegrityViolationException e) {
            log.info("Otro consumidor registro este evento en paralelo, se ignora. eventId={}",
                    evento.eventId());
            return;
        }

        log.info("PedidoCreado procesado OK eventId={}", evento.eventId());
    }
}
