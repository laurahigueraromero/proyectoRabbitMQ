package com.example.rabbit.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Component
public class PedidoCreadoDlqListener {

    private static final Logger log = LoggerFactory.getLogger(PedidoCreadoDlqListener.class);

    /**
     * Consumidor de la Dead-Letter Queue.
     *
     * Recibimos el Message CRUDO (sin deserializar a PedidoCreado) a proposito:
     * un mensaje puede acabar aqui justamente porque su JSON estaba corrupto y no
     * se podia convertir. Aqui solo queremos inspeccionarlo y dejar constancia.
     *
     * RabbitMQ, al hacer dead-letter, anade el header "x-death" con el historial:
     * cuantas veces murio, en que cola, por que motivo (rejected, expired, maxlen)
     * y cuando. Es oro puro para depurar en una entrevista / en produccion.
     */
    @RabbitListener(queues = "${app.messaging.queue.pedido-creado-dlq}")
    public void onDeadLetter(Message message) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        Map<String, Object> headers = message.getMessageProperties().getHeaders();

        Object xDeath = headers.get("x-death");
        long muertes = 0;
        String motivo = "desconocido";
        String colaOrigen = "desconocida";
        if (xDeath instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof Map<?, ?> first) {
            Object count = first.get("count");
            if (count instanceof Number n) {
                muertes = n.longValue();
            }
            motivo = String.valueOf(first.get("reason"));
            colaOrigen = String.valueOf(first.get("queue"));
        }

        log.warn("=== MENSAJE EN DLQ === messageId={} motivo={} colaOrigen={} vecesMuerto={}\n  body={}",
                message.getMessageProperties().getMessageId(), motivo, colaOrigen, muertes, body);

        // En un sistema real: guardar en una tabla de "mensajes fallidos", alertar,
        // ofrecer un endpoint para reprocesar manualmente, etc.
    }
}
