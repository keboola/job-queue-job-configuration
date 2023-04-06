<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Configuration\Runtime;

use Keboola\JobQueue\JobConfiguration\Configuration\Runtime\Backend;
use PHPUnit\Framework\TestCase;

class BackendTest extends TestCase
{
    public function testFromArray(): void
    {
        $data = [
            'type' => 'testType',
            'context' => 'testContext',
        ];

        $backend = Backend::fromArray($data);

        self::assertSame('testType', $backend->type);
        self::assertSame('testContext', $backend->context);
    }

    public function testToArray(): void
    {
        $backend = new Backend(
            type: 'testType',
            context: 'testContext',
        );

        self::assertSame([
            'type' => 'testType',
            'context' => 'testContext',
        ], $backend->toArray());
    }
}
