package com.example.rabbit.producer;

import com.example.rabbit.domain.PedidoCreado;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * PRODUCTOR. Unico punto de la app que publica el evento PedidoCreado en RabbitMQ.
 *
 * Su trabajo: recibir un objeto de dominio y entregarlo al broker, sin saber quien
 * lo consumira ni esperar respuesta (mensajeria asincrona, productor y consumidor
 * desacoplados).
 *
 * Relacion con RabbitMQ:
 *   - Usa RabbitTemplate, el cliente de publicacion de Spring AMQP (configurado en
 *     RabbitConfig con el converter JSON y el returns-callback).
 *   - convertAndSend(exchange, routingKey, objeto, postProcessor):
 *       1. el MessageConverter serializa el objeto Java a JSON en el cuerpo del mensaje;
 *       2. el postProcessor (lambda) ajusta las properties AMQP del "sobre" antes de enviar;
 *       3. se publica SIEMPRE a un EXCHANGE (nunca directo a una cola). El exchange, segun
 *          la routing key "pedido.creado" y sus bindings, decide a que cola(s) va.
 *   - Properties que fija el postProcessor:
 *       * messageId = eventId  -> el id del evento viaja en el sobre; el consumidor lo usa
 *                                 para deduplicar (idempotencia).
 *       * contentType = application/json
 *       * header x-event-type = "PedidoCreado" -> permite enrutar/filtrar por tipo de evento.
 *       * deliveryMode = PERSISTENT -> el broker escribe el mensaje a disco; sobrevive a un
 *                                      reinicio del broker (junto con la cola durable).
 *
 */
@Component
public class PedidoEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PedidoEventPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final String exchange;
    private final String routingKey;

    public PedidoEventPublisher(RabbitTemplate rabbitTemplate,
                                @Value("${app.messaging.exchange}") String exchange,
                                @Value("${app.messaging.routing-key.pedido-creado}") String routingKey) {
        this.rabbitTemplate = rabbitTemplate;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    public void publicar(PedidoCreado evento) {
        rabbitTemplate.convertAndSend(exchange, routingKey, evento, message -> {
            // messageId = eventId: nos dara idempotencia "gratis" en el Paso 6
            message.getMessageProperties().setMessageId(evento.eventId());
            message.getMessageProperties().setContentType("application/json");
            message.getMessageProperties().setHeader("x-event-type", "PedidoCreado");
            // PERSISTENT: el mensaje se escribe a disco; si el broker reinicia, no se pierde
            message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            return message;
        });
        log.info("Publicado PedidoCreado eventId={} pedidoId={} exchange={} rk={}",
                evento.eventId(), evento.pedidoId(), exchange, routingKey);
    }
}
