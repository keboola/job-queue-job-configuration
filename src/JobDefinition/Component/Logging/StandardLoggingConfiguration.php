<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Logging;

class StandardLoggingConfiguration implements LoggingConfigurationInterface
{
    public static function fromArray(array $data): self
    {
        return new self();
    }

    public function toArray(): array
    {
        return [
            'type' => 'standard',
        ];
    }
}
