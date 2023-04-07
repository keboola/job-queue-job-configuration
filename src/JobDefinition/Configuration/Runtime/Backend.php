<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime;

readonly class Backend
{
    public function __construct(
        public ?string $type,
        public ?string $context,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            type: $data['type'] ?? null,
            context: $data['context'] ?? null,
        );
    }

    public function toArray(): array
    {
        $data = [];

        if ($this->type !== null) {
            $data['type'] = $this->type;
        }

        if ($this->context !== null) {
            $data['context'] = $this->context;
        }

        return $data;
    }
}
