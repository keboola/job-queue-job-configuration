<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Configuration\Runtime;

use Keboola\JobQueue\JobConfiguration\Configuration\Runtime\Backend;
use Keboola\JobQueue\JobConfiguration\Configuration\Runtime\Runtime;
use PHPUnit\Framework\TestCase;

class RuntimeTest extends TestCase
{
    public function testFromArray(): void
    {
        $data = [
            'safe' => true,
            'image_tag' => false,
            'use_file_storage_only' => true,
            'backend' => [
                'type' => 'testType',
                'context' => 'testContext',
            ],
            'foo' => 'bar',
        ];

        $runtime = Runtime::fromArray($data);

        self::assertTrue($runtime->safe);
        self::assertFalse($runtime->imageTag);
        self::assertTrue($runtime->useFileStorageOnly);
        self::assertInstanceOf(Backend::class, $runtime->backend);
        self::assertSame('testType', $runtime->backend->type);
        self::assertSame('testContext', $runtime->backend->context);
        self::assertSame(['foo' => 'bar'], $runtime->extraProps);
    }

    public function testToArray(): void
    {
        $backend = new Backend(
            type: 'testType',
            context: 'testContext',
        );

        $runtime = new Runtime(
            safe: true,
            imageTag: false,
            useFileStorageOnly: true,
            backend: $backend,
            extraProps: ['foo' => 'bar'],
        );

        self::assertSame([
            'safe' => true,
            'image_tag' => false,
            'use_file_storage_only' => true,
            'backend' => [
                'type' => 'testType',
                'context' => 'testContext',
            ],
            'foo' => 'bar',
        ], $runtime->toArray());
    }
}
