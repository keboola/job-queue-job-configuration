<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors;

readonly class ProcessorDefinition
{
    public function __construct(
        public string $component,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            component: $data['component'] ?? '',
        );
    }

    public function toArray(): array
    {
        return [
            'component' => $this->component,
        ];
    }
}
