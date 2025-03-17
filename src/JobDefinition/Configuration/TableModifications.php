<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration;

enum TableModifications: string
{
    case NONE = 'none';
    case NON_DESTRUCTIVE = 'non-destructive';
    case ALL = 'all';

    /** @return string[] */
    public static function values(): array
    {
        return array_column(self::cases(), 'value');
    }
}
