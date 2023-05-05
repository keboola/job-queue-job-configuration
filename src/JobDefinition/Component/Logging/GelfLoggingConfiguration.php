<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Logging;

class GelfLoggingConfiguration implements LoggingConfigurationInterface
{
    /** @var non-empty-string */
    public readonly string $port;

    public function __construct(
        /** @var non-empty-string */
        public readonly string $type,
    ) {
        $this->port = $this->type === 'http' ? '12002' : '12001';
    }

    public static function fromArray(array $data): self
    {
        return new self(
            $data['gelf_server_type'] ?? 'udp',
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
