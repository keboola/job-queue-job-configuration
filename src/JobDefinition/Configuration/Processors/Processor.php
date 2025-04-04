<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors;

readonly class Processor
{
    public function __construct(
        public ProcessorDefinition $definition,
        public array $parameters = [],
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            definition: ProcessorDefinition::fromArray($data['definition'] ?? []),
            parameters: $data['parameters'] ?? [],
        );
    }

    public function toArray(): array
    {
        return array_filter([
            'definition' => $this->definition->toArray(),
            'parameters' => $this->parameters ?? [],
        ]);
    }
}
