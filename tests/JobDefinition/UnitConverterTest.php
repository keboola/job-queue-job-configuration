<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition;

use Generator;
use Keboola\JobQueue\JobConfiguration\Exception\InvalidUnitFormatException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\UnitConverter;
use PHPUnit\Framework\TestCase;

class UnitConverterTest extends TestCase
{
    public function testConversionToBytes(): void
    {
        self::assertSame(10000000, UnitConverter::connectionMemoryLimitToBytes('10m'));
        self::assertSame(10000000, UnitConverter::connectionMemoryLimitToBytes('10M'));
        self::assertSame(10000000000, UnitConverter::connectionMemoryLimitToBytes('10g'));
        self::assertSame(10000000000, UnitConverter::connectionMemoryLimitToBytes('10G'));
    }

    /** @dataProvider invalidUnitProvider */
    public function testConversionInvalid(string $data, string $expectedError): void
    {
        $this->expectException(InvalidUnitFormatException::class);
        $this->expectExceptionMessage($expectedError);
        UnitConverter::connectionMemoryLimitToBytes($data);
    }

    public static function invalidUnitProvider(): Generator
    {
        yield 'invalid format' => [
            'unknown',
            'Value "unknown" is not understood',
        ];
        yield 'invalid unit' => [
            '10T',
            'Value "10t" is not understood',
        ];
    }
}
