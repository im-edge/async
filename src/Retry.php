<?php

namespace IMEdge\Async;

use Closure;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;
use RuntimeException;
use Throwable;

class Retry
{
    protected ?int $failedAt = null;
    protected ?int $currentRetryInterval = null;

    protected function __construct(
        protected Closure $callback,
        protected string $label,
        public readonly int $retryIntervalSeconds,
        public readonly ?int $burstIntervalSeconds = null,
        public readonly ?int $burstDuration = null,
        protected readonly ?LoggerInterface $logger = null,
    ) {
        if ($this->burstIntervalSeconds !== null) {
            if ($this->burstDuration === null) {
                throw new RuntimeException('Cannot launch a Retry instance with burst interval and no burst duration');
            }
            if ($this->burstIntervalSeconds === $this->retryIntervalSeconds) {
                throw new RuntimeException('Burst interval must not equal retry interval, but can be null');
            }
        }
    }

    public static function forever(
        Closure $callback,
        string $label,
        int $retryIntervalSeconds,
        ?int $burstIntervalSeconds = null,
        ?int $burstDuration = null,
        ?LoggerInterface $logger = null,
    ): void {
        (new Retry(
            $callback,
            $label,
            $retryIntervalSeconds,
            $burstIntervalSeconds,
            $burstDuration,
            $logger
        ))->launch();
    }

    protected function launch(): void
    {
        try {
            $callback = $this->callback;
            $callback();
            $this->failedAt = null;
            $this->currentRetryInterval = null;
        } catch (Throwable $e) {
            $now = hrtime(true);
            if ($this->failedAt === null) {
                $this->setInitialError($e);
                $this->failedAt = $now;
            } elseif ($this->currentRetryInterval === $this->burstIntervalSeconds) {
                if ($this->failedAt < ($now - $this->burstDuration * 1_000_000_000)) {
                    $this->switchToNormalRetryInterval($e);
                }
            }
            if ($this->currentRetryInterval) {
                EventLoop::delay($this->currentRetryInterval, $this->launch(...));
            }
        }
    }

    protected function switchToNormalRetryInterval(Throwable $e): void
    {
        $this->currentRetryInterval = $this->retryIntervalSeconds;
        $this->logger?->error(sprintf(
            "%s still failing after %ss, slowing down to one attempt every %ds: %s (%s:%d)",
            $this->label,
            $this->burstDuration,
            $this->retryIntervalSeconds,
            $e->getMessage(),
            $e->getFile(),
            $e->getLine()
        ));
    }

    protected function setInitialError(Throwable $e): void
    {
        $this->currentRetryInterval = $this->burstIntervalSeconds ?? $this->retryIntervalSeconds;
        $this->logger?->error(sprintf(
            "%s failed, I'll continue to retry every %ds: %s (%s:%d)",
            $this->label,
            $this->currentRetryInterval,
            $e->getMessage(),
            $e->getFile(),
            $e->getLine()
        ) . $e->getTraceAsString());
    }
}
