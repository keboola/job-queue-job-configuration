<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobStorageApiClient;

use InvalidArgumentException;
use Throwable;

/**
 * @phpstan-type ArrayOptions array{
 *      run_id: non-empty-string,
 *      branch_id: non-empty-string,
 *      use_branch_storage: bool,
 *      backoff_max_tries?: null|non-negative-int,
 *      backend?: null|array{
 *          context?: null|string,
 *          size?: null|string,
 *      },
 *  }
 */
readonly class JobStorageApiClientOptions
{
    public function __construct(
        /** @var non-empty-string */
        public string $runId,
        /** @var non-empty-string */
        public string $branchId,
        public bool $useBranchStorage,
        /** @var null|non-negative-int */
        public ?int $backoffMaxTries = null,
        public ?string $backendSize = null,
        public ?string $backendContext = null,
    ) {
    }

    /**
     * @param ArrayOptions $data
     */
    public static function fromArray(array $data): self
    {
        try {
            return new self(
                $data['run_id'],
                $data['branch_id'],
                $data['use_branch_storage'],
                $data['backoff_max_tries'] ?? null,
                $data['backend']['size'] ?? null,
                $data['backend']['context'] ?? null,
            );
        } catch (Throwable $e) {
            throw new InvalidArgumentException(
                'Invalid job storage client options: ' . $e->getMessage(),
                previous: $e,
            );
        }
    }

    /**
     * @return ArrayOptions
     */
    public function toArray(): array
    {
        return [
            'run_id' => $this->runId,
            'branch_id' => $this->branchId,
            'use_branch_storage' => $this->useBranchStorage,
            'backoff_max_tries' => $this->backoffMaxTries,
            'backend' => [
                'size' => $this->backendSize,
                'context' => $this->backendContext,
            ],
        ];
    }
}
