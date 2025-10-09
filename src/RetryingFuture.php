<?php

namespace IMEdge\Async;

use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Future;
use Closure;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;
use RuntimeException;
use Throwable;

class RetryingFuture
{
    use ForbidCloning;
    use ForbidSerialization;

    protected ?int $failedAt = null;
    protected ?float $currentRetryInterval = null;
    /** @var DeferredFuture<mixed> */
    protected DeferredFuture $deferred;
    protected DeferredCancellation $canceller;

    public function __construct(
        protected Closure $callback,
        protected string $label,
        public readonly float $retryIntervalSeconds,
        public readonly ?float $burstIntervalSeconds = null,
        public readonly ?float $burstDuration = null,
        protected readonly ?LoggerInterface $logger = null,
    ) {
        $this->assertParametersAreValid();
        $this->canceller = new DeferredCancellation();
        $this->deferred = new DeferredFuture();
    }

    public function awaitSuccess(): mixed
    {
        $future = $this->deferred->getFuture();
        EventLoop::defer($this->launch(...));
        return $future->await($this->canceller->getCancellation());
    }

    public function cancel(Throwable $reason): void
    {
        $this->canceller->cancel($reason);
    }

    protected function launch(): void
    {
        try {
            $callback = $this->callback;
            $result = $callback();
            while ($result instanceof Future) {
                $result = $result->await($this->canceller->getCancellation());
            }
            $this->failedAt = null;
            $this->currentRetryInterval = null;
            $this->deferred->complete($result);
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
            "%s still failing after %.2fs, going on with to one attempt every %.2fs: %s (%s:%d)",
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
            "%s failed, I'll continue to retry every %.2fs: %s (%s:%d)",
            $this->label,
            $this->currentRetryInterval,
            $e->getMessage(),
            $e->getFile(),
            $e->getLine()
        ));
    }

    protected function assertParametersAreValid(): void
    {
        if ($this->retryIntervalSeconds <= 0) {
            throw new RuntimeException(
                "Retry ($this->label) requires a positive retry interval"
            );
        }
        if ($this->burstIntervalSeconds !== null) {
            if ($this->burstDuration === null) {
                throw new RuntimeException(
                    "Cannot launch Retry ($this->label) with burst interval and no burst duration"
                );
            }
            if ($this->burstIntervalSeconds === $this->retryIntervalSeconds) {
                throw new RuntimeException(
                    "Burst interval ($this->label) must not equal retry interval, but can be null"
                );
            }
            if ($this->burstIntervalSeconds <= 0) {
                throw new RuntimeException(
                    "Retry ($this->label) requires a positive burst interval, when specified"
                );
            }
        }
    }
}
