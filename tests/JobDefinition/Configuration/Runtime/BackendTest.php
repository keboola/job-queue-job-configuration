<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Runtime;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Backend;
use PHPUnit\Framework\TestCase;

class BackendTest extends TestCase
{
    public function testFromArray(): void
    {
        $data = [
            'type' => 'testType',
            'container_type' => 'testContainerType',
            'context' => 'testContext',
        ];

        $backend = Backend::fromArray($data);

        self::assertSame('testType', $backend->type);
        self::assertSame('testContainerType', $backend->containerType);
        self::assertSame('testContext', $backend->context);
    }

    public function testToArray(): void
    {
        $backend = new Backend(
            type: 'testType',
            containerType: 'testContainerType',
            context: 'testContext',
        );

        self::assertSame([
            'type' => 'testType',
            'container_type' => 'testContainerType',
            'context' => 'testContext',
        ], $backend->toArray());
    }
}
