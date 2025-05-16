<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

interface SecretsRedactorInterface
{
    public function redactSecrets(string $text): string;
}
