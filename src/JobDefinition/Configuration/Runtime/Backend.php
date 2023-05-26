<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime;

readonly class Backend
{
    public function __construct(
        public ?string $type = null,
        public ?string $containerType = null,
        public ?string $context = null,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            type: $data['type'] ?? null,
            containerType: $data['container_type'] ?? null,
            context: $data['context'] ?? null,
        );
    }

    public function toArray(): array
    {
        return [
            'type' => $this->type,
            'container_type' => $this->containerType,
            'context' => $this->context,
        ];
    }
}
