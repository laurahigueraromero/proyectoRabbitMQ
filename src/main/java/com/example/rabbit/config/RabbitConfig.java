package com.example.rabbit.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    /**
     * Exchange TOPIC y durable.
     *
     * - topic: enruta segun un patron sobre la routing key ("pedido.*", "pedido.#").
     *   Nos permite tener en el futuro varios eventos ("pedido.creado", "pedido.cancelado")
     *   y que cada consumidor se suscriba solo a los que le interesan.
     * - durable: la definicion del exchange sobrevive a un reinicio del broker.
     *
     * De momento solo declaramos el exchange. Las colas y bindings los creara el
     * consumidor en el Paso 3 (quien consume es quien declara su cola).
     */
    @Bean
    TopicExchange pedidosExchange(@Value("${app.messaging.exchange}") String exchangeName) {
        return ExchangeBuilder.topicExchange(exchangeName).durable(true).build();
    }

    /**
     * La COLA la declara quien consume: es "su" buzon. Durable = su definicion y sus
     * mensajes persistentes sobreviven a un reinicio del broker.
     *
     * Ahora ademas lleva dos ARGUMENTOS de "dead-lettering":
     *   - x-dead-letter-exchange   -> a que exchange reenviar un mensaje cuando "muere"
     *   - x-dead-letter-routing-key-> con que routing key reenviarlo
     * Un mensaje "muere" cuando: se rechaza sin requeue (nuestro caso al agotar
     * reintentos), caduca por TTL, o la cola esta llena.
     *
     * OJO: estos argumentos son parte de la definicion de la cola. Si la cola ya
     * existe en el broker SIN ellos, RabbitMQ rechaza la redeclaracion
     * (PRECONDITION_FAILED). Hay que borrar la cola vieja antes de reiniciar.
     */
    @Bean
    Queue pedidoCreadoQueue(@Value("${app.messaging.queue.pedido-creado}") String queueName,
                            @Value("${app.messaging.dlx}") String dlxName,
                            @Value("${app.messaging.routing-key.pedido-creado-dlq}") String dlqRoutingKey) {
        return QueueBuilder.durable(queueName)
                .deadLetterExchange(dlxName)
                .deadLetterRoutingKey(dlqRoutingKey)
                .build();
    }

    /**
     * El BINDING es la regla que conecta exchange -> cola. Con exchange topic, la
     * routing key del binding puede llevar comodines; aqui usamos la exacta
     * "pedido.creado", asi que solo ese evento entra en esta cola.
     */
    @Bean
    Binding pedidoCreadoBinding(Queue pedidoCreadoQueue,
                                TopicExchange pedidosExchange,
                                @Value("${app.messaging.routing-key.pedido-creado}") String routingKey) {
        return BindingBuilder.bind(pedidoCreadoQueue).to(pedidosExchange).with(routingKey);
    }

    // ----------------------------------------------------------------------------
    // Dead-Letter: exchange + cola donde acaban los mensajes que agotan reintentos
    // ----------------------------------------------------------------------------

    /**
     * Exchange de mensajes muertos. Lo hacemos DIRECT: enruta por routing key exacta.
     * Asi, si manana hay varios eventos, cada uno puede tener su propia DLQ colgando
     * de este mismo DLX con su routing key.
     */
    @Bean
    DirectExchange pedidosDlx(@Value("${app.messaging.dlx}") String dlxName) {
        return ExchangeBuilder.directExchange(dlxName).durable(true).build();
    }

    /** La cola donde se acumulan los PedidoCreado que no se pudieron procesar. */
    @Bean
    Queue pedidoCreadoDlq(@Value("${app.messaging.queue.pedido-creado-dlq}") String dlqName) {
        return QueueBuilder.durable(dlqName).build();
    }

    /** Conecta el DLX con la DLQ usando la misma routing key que pusimos en la cola principal. */
    @Bean
    Binding pedidoCreadoDlqBinding(Queue pedidoCreadoDlq,
                                   DirectExchange pedidosDlx,
                                   @Value("${app.messaging.routing-key.pedido-creado-dlq}") String dlqRoutingKey) {
        return BindingBuilder.bind(pedidoCreadoDlq).to(pedidosDlx).with(dlqRoutingKey);
    }

    /**
     * Convierte el objeto Java a/desde JSON en el cuerpo del mensaje AMQP.
     * Reutilizamos el ObjectMapper que ya configura Spring Boot (incluye soporte
     * para java.time.Instant via jackson-datatype-jsr310).
     */
    @Bean
    MessageConverter jsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    /**
     * RabbitTemplate = el cliente para PUBLICAR. Le enchufamos el converter JSON
     * y activamos returns para enterarnos si un mensaje sale "no enrutable".
     */
    @Bean
    RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter);
        template.setReturnsCallback(returned ->
                org.slf4j.LoggerFactory.getLogger(RabbitConfig.class).warn(
                        "Mensaje NO enrutado: exchange={} routingKey={} replyText={}",
                        returned.getExchange(), returned.getRoutingKey(), returned.getReplyText()));
        return template;
    }
}
