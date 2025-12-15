<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Logging;

class GelfLoggingConfiguration implements LoggingConfigurationInterface
{
    public function __construct(
        /** @var non-empty-string */
        public readonly string $type,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            (string) ($data['gelf_server_type'] ?? 'tcp'),
        );
    }

    public function toArray(): array
    {
        return [
            'type' => 'gelf',
            'gelf_server_type' => $this->type,
        ];
    }
}
