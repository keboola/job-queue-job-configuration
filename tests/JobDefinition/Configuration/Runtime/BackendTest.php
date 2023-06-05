<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Runtime;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Backend;
use PHPUnit\Framework\TestCase;

class BackendTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $backend = new Backend();

        self::assertNull($backend->type);
        self::assertNull($backend->context);
    }

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
