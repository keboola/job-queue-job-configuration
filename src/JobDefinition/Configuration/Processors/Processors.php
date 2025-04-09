<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors;

readonly class Processors
{
    /**
     * @param array<Processor> $before
     * @param array<Processor> $after
     */
    public function __construct(
        public array $before = [],
        public array $after = [],
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            before: array_map(fn($p) => Processor::fromArray($p), $data['before'] ?? []),
            after: array_map(fn($p) => Processor::fromArray($p), $data['after'] ?? []),
        );
    }

    public function toArray(): array
    {
        return array_filter([
            'before' => array_map(fn(Processor $p) => $p->toArray(), $this->before),
            'after' => array_map(fn(Processor $p) => $p->toArray(), $this->after),
        ]);
    }
}
