<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts;

readonly class Custom
{
    public function __construct(
        public bool $enabled = false,
        public CustomFilter $filter = new CustomFilter(),
    ) {
    }

    public static function fromArray(array $data): self
    {
        /** @var array $filter */
        $filter = $data['filter'] ?? [];

        return new self(
            enabled: (bool) ($data['enabled'] ?? false),
            filter: CustomFilter::fromArray($filter),
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
