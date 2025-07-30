<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Artifacts;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Shared;
use PHPUnit\Framework\TestCase;

class SharedTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $shared = new Shared();

        self::assertFalse($shared->enabled);
    }

    public function testConstructorWithParameters(): void
    {
        $shared = new Shared(enabled: true);

        self::assertTrue($shared->enabled);
    }

    public function testFromEmptyArray(): void
    {
        $shared = Shared::fromArray([]);

        self::assertFalse($shared->enabled);
    }

    public function testFromArray(): void
    {
        $data = ['enabled' => true];
        $shared = Shared::fromArray($data);

        self::assertTrue($shared->enabled);
    }

    public function testToArray(): void
    {
        $shared = new Shared(enabled: true);

        $expected = ['enabled' => true];

        self::assertSame($expected, $shared->toArray());
    }

    public function testToArrayWithDefaults(): void
    {
        $shared = new Shared();

        $expected = ['enabled' => false];

        self::assertSame($expected, $shared->toArray());
    }
}
