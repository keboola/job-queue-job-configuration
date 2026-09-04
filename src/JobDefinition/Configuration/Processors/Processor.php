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
            definition: ProcessorDefinition::fromArray($data['definition']),
            parameters: $data['parameters'] ?? [],
        );
    }

    public function toArray(): array
    {
        return [
            'definition' => $this->definition->toArray(),
            // `parameters` must be emitted even when empty: a component's config schema applies its
            // defaults only to keys already present in its input, so a missing key makes every
            // defaulted parameter unresolvable inside the processor. ConfigFileManager serializes
            // the empty array as `{}`.
            'parameters' => $this->parameters,
        ];
    }
}
