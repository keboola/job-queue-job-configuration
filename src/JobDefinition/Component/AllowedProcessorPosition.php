<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Component;

enum AllowedProcessorPosition: string
{
    case ANY = 'any';
    case BEFORE = 'before';
    case AFTER = 'after';

    /** @return string[] */
    public static function values(): array
    {
        return array_column(self::cases(), 'value');
    }
}
