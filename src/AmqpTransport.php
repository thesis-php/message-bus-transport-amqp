<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Thesis\Amqp\Channel;
use Thesis\Amqp\Client;
use Thesis\Amqp\DeliveryMessage;
use Thesis\Amqp\PublishMessage;
use Thesis\Message\Command;
use Thesis\MessageBus\Envelope;
use Thesis\MessageBus\Transport\Transport;

/**
 * @api
 */
final class AmqpTransport implements Transport
{
    /**
     * @var SimpleMutex<Channel>
     */
    private SimpleMutex $publishChannel;

    public function __construct(
        private readonly Client $client,
        private readonly ExchangeNaming $exchangeNaming = new MessageClassBasedExchangeNaming(),
        private readonly AmqpEnvelopeEncoder $encoder = new DefaultAmqpEnvelopeEncoder(),
    ) {
        $this->publishChannel = new SimpleMutex(
            factory: function (): Channel {
                $channel = $this->client->channel();
                $channel->confirmSelect();

                return $channel;
            },
            isHit: static fn(Channel $channel): bool => !$channel->isClosed(),
        );
    }

    public function setup(string $endpoint, array $localMessages): void
    {
        $channel = $this->client->channel();
        $channel->queueDeclare($endpoint, durable: true);

        foreach ($localMessages as $localMessage) {
            $exchange = $this->exchangeNaming->nameExchange($localMessage);
            $this->declareExchange($channel, $exchange);
            $channel->queueBind($endpoint, $exchange);
        }

        $channel->close();
    }

    /**
     * @var array<non-empty-string, true>
     */
    private array $declaredExchanges = [];

    /**
     * @param non-empty-string $exchange
     */
    private function declareExchange(Channel $channel, string $exchange): void
    {
        if (!isset($this->declaredExchanges[$exchange])) {
            $channel->exchangeDeclare($exchange, exchangeType: 'fanout', durable: true);
            $this->declaredExchanges[$exchange] = true;
        }
    }

    public function publish(array $envelopes): void
    {
        $channel = $this->publishChannel->get();
        $channel
            ->publishBatch(array_map(
                function (Envelope $envelope) use ($channel): PublishMessage {
                    $exchange = $this->exchangeNaming->nameExchange($envelope->messageClass);
                    $this->declareExchange($channel, $exchange);

                    return new PublishMessage(
                        message: $this->encoder->encodeEnvelope($envelope),
                        exchange: $exchange,
                        mandatory: $envelope->message instanceof Command,
                    );
                },
                $envelopes,
            ))
            ->await()
            ->ensureAllPublished();
    }

    public function consume(string $endpoint, \Closure $handler): \Closure
    {
        $client = new Client($this->client->config);
        $channel = $client->channel();
        $channel->qos(prefetchCount: 1);

        $consumerTag = $channel->consume(
            callback: function (DeliveryMessage $deliveryMessage) use ($handler): void {
                $handler($this->encoder->decodeEnvelope($deliveryMessage->message));
                $deliveryMessage->ack();
            },
            queue: $endpoint,
        );

        return static function () use ($client, $channel, $consumerTag): void {
            $channel->cancel($consumerTag);
            $channel->close();
            $client->disconnect();
        };
    }
}
