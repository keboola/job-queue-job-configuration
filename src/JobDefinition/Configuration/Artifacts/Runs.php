<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts;

readonly class Runs
{
    public function __construct(
        public bool $enabled = false,
        public RunsFilter $filter = new RunsFilter(),
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            enabled: $data['enabled'] ?? false,
            filter: RunsFilter::fromArray($data['filter'] ?? []),
        );
    }

    public function toArray(): array
    {
        $result = [
            'enabled' => $this->enabled,
        ];

        $filterArray = $this->filter->toArray();
        if (count($filterArray) !== 0) {
            $result['filter'] = $filterArray;
        }

        return $result;
    }
}
