<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Configuration;

readonly class Configuration
{
    public function __construct(
        public array $parameters,
        public Storage\Storage $storage,
        public array $processors,
        public ?Runtime\Runtime $runtime,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            parameters: $data['parameters'] ?? [],
            storage: Storage\Storage::fromArray($data['storage'] ?? []),
            processors: $data['processors'] ?? [],
            runtime: isset($data['runtime']) ? Runtime\Runtime::fromArray($data['runtime']) : null,
        );
    }
}
