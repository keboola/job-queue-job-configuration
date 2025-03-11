<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials;

enum Type: string
{
    case SNOWFLAKE = 'snowflake';
}
