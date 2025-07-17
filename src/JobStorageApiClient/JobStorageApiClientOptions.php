<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobStorageApiClient;

use InvalidArgumentException;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
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
    public const CONFIG_KEY = 'storage_client_options';

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

    public static function configDefinition(): ArrayNodeDefinition
    {
        return (new ArrayNodeDefinition(self::CONFIG_KEY))
            ->ignoreExtraKeys()
            ->children()
                ->scalarNode('run_id')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('branch_id')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->booleanNode('use_branch_storage')
                    ->isRequired()
                ->end()
                ->integerNode('backoff_max_tries')
                    ->defaultNull()
                    ->min(0)
                    ->beforeNormalization()
                        ->ifNull()
                        ->thenUnset()
                    ->end()
                ->end()
                ->arrayNode('backend')
                    ->isRequired()
                    ->ignoreExtraKeys()
                    ->children()
                        ->scalarNode('size')
                            ->defaultNull()
                        ->end()
                        ->scalarNode('context')
                            ->defaultNull()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ;
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
