<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts;

readonly class Artifacts
{
    public function __construct(
        public Options $options = new Options(),
        public Runs $runs = new Runs(),
        public Custom $custom = new Custom(),
        public Shared $shared = new Shared(),
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            options: Options::fromArray($data['options'] ?? []),
            runs: Runs::fromArray($data['runs'] ?? []),
            custom: Custom::fromArray($data['custom'] ?? []),
            shared: Shared::fromArray($data['shared'] ?? []),
        );
    }

    public function toArray(): array
    {
        return [
            'options' => $this->options->toArray(),
            'runs' => $this->runs->toArray(),
            'custom' => $this->custom->toArray(),
            'shared' => $this->shared->toArray(),
        ];
    }
}
