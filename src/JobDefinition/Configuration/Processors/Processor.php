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
        /** @var ProcessorDefinitionArray $definition */
        $definition = $data['definition'];
        /** @var array $parameters */
        $parameters = $data['parameters'] ?? [];

        return new self(
            definition: ProcessorDefinition::fromArray($definition),
            parameters: $parameters,
        );
    }

    public function toArray(): array
    {
        return array_filter([
            'definition' => $this->definition->toArray(),
            'parameters' => $this->parameters,
        ]);
    }
}
