<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\Attribute;

use Attribute;

#[Attribute]
class UseAzureProject
{
    public function __construct(public bool $useMasterToken = false)
    {
    }
}
