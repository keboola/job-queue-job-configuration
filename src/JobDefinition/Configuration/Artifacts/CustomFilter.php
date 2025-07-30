<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts;

readonly class CustomFilter
{
    public function __construct(
        public ?string $componentId = null,
        public ?string $configId = null,
        public ?string $branchId = null,
        public ?string $dateSince = null,
        public ?int $limit = null,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            componentId: $data['component_id'] ?? null,
            configId: $data['config_id'] ?? null,
            branchId: $data['branch_id'] ?? null,
            dateSince: $data['date_since'] ?? null,
            limit: $data['limit'] ?? null,
        );
    }

    public function toArray(): array
    {
        return array_filter([
            'component_id' => $this->componentId,
            'config_id' => $this->configId,
            'branch_id' => $this->branchId,
            'date_since' => $this->dateSince,
            'limit' => $this->limit,
        ], static fn($value) => $value !== null);
    }
}
