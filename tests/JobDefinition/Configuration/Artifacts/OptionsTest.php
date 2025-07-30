<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Artifacts;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Options;
use PHPUnit\Framework\TestCase;

class OptionsTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $options = new Options();

        self::assertTrue($options->zip);
    }

    public function testConstructorWithParameters(): void
    {
        $options = new Options(zip: false);

        self::assertFalse($options->zip);
    }

    public function testFromEmptyArray(): void
    {
        $options = Options::fromArray([]);

        self::assertTrue($options->zip);
    }

    public function testFromArray(): void
    {
        $data = ['zip' => false];
        $options = Options::fromArray($data);

        self::assertFalse($options->zip);
    }

    public function testToArray(): void
    {
        $options = new Options(zip: false);

        $expected = ['zip' => false];

        self::assertSame($expected, $options->toArray());
    }

    public function testToArrayWithDefaults(): void
    {
        $options = new Options();

        $expected = ['zip' => true];

        self::assertSame($expected, $options->toArray());
    }
}
