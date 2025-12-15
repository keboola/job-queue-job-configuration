<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts;

readonly class RunsFilter
{
    public function __construct(
        public ?string $dateSince = null,
        public ?int $limit = null,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            dateSince: isset($data['date_since']) ? (string) $data['date_since'] : null,
            limit: isset($data['limit']) ? (int) $data['limit'] : null,
        );
    }

    public function toArray(): array
    {
        return array_filter([
            'date_since' => $this->dateSince,
            'limit' => $this->limit,
        ], static fn($value) => $value !== null);
    }
}
