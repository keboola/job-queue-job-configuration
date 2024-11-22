<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration;

enum DataTypeSupport: string
{
    case AUTHORITATIVE = 'authoritative';
    case HINTS = 'hints';
    case NONE = 'none';

    /** @return string[] */
    public static function values(): array
    {
        return array_column(self::cases(), 'value');
    }
}
