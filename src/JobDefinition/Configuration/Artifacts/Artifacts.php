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
        /** @var array $options */
        $options = $data['options'] ?? [];
        /** @var array $runs */
        $runs = $data['runs'] ?? [];
        /** @var array $custom */
        $custom = $data['custom'] ?? [];
        /** @var array $shared */
        $shared = $data['shared'] ?? [];

        return new self(
            options: Options::fromArray($options),
            runs: Runs::fromArray($runs),
            custom: Custom::fromArray($custom),
            shared: Shared::fromArray($shared),
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
