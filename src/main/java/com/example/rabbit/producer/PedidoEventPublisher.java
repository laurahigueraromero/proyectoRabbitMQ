package com.example.rabbit.producer;

import com.example.rabbit.domain.PedidoCreado;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
