<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Logging;

interface LoggingConfigurationInterface
{
    public function toArray(): array;
}
