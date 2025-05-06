<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Transport\Amqp;

use Amp\Cancellation;
use Amp\Future;
use function Amp\async;

/**
 * @api
 * @template T
 */
final class SimpleMutex
{
    /**
     * @var ?Future<T>
     */
    private ?Future $future = null;

    /**
     * @var ?T
     */
    private mixed $value = null;

    /**
     * @var \Closure(T): bool
     */
    private readonly \Closure $isHit;

    /**
     * @param \Closure(): T $factory
     * @param ?\Closure(T): bool $isHit
     */
    public function __construct(
        private readonly \Closure $factory,
        ?\Closure $isHit = null,
        private readonly ?Cancellation $cancellation = null,
    ) {
        $this->isHit = $isHit ?? static fn(): true => true;
    }

    /**
     * @return T
     */
    public function get(): mixed
    {
        if ($this->value !== null && ($this->isHit)($this->value)) {
            return $this->value;
        }

        $this->future ??= async($this->factory);

        try {
            return $this->value = $this->future->await($this->cancellation);
        } finally {
            $this->future = null;
        }
    }
}
