<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\Attribute;

use Attribute;

#[Attribute]
class UseGCPProject
{
    public function __construct(public bool $useMasterToken = false)
    {
    }
}
