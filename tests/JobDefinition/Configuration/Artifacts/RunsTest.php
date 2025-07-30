<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Artifacts;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Runs;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\RunsFilter;
use PHPUnit\Framework\TestCase;

class RunsTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $runs = new Runs();

        self::assertFalse($runs->enabled);
        self::assertEquals(new RunsFilter(), $runs->filter);
    }

    public function testConstructorWithParameters(): void
    {
        $filter = new RunsFilter(dateSince: '2023-01-01', limit: 100);
        $runs = new Runs(enabled: true, filter: $filter);

        self::assertTrue($runs->enabled);
        self::assertEquals($filter, $runs->filter);
    }

    public function testFromEmptyArray(): void
    {
        $runs = Runs::fromArray([]);

        self::assertFalse($runs->enabled);
        self::assertEquals(new RunsFilter(), $runs->filter);
    }

    public function testFromArray(): void
    {
        $data = [
            'enabled' => true,
            'filter' => [
                'date_since' => '2023-01-01',
                'limit' => 100,
            ],
        ];
        $runs = Runs::fromArray($data);

        self::assertTrue($runs->enabled);
        self::assertEquals(RunsFilter::fromArray($data['filter']), $runs->filter);
    }

    public function testFromArrayOnlyEnabled(): void
    {
        $data = ['enabled' => true];
        $runs = Runs::fromArray($data);

        self::assertTrue($runs->enabled);
        self::assertEquals(new RunsFilter(), $runs->filter);
    }

    public function testToArrayDefaultValues(): void
    {
        $runs = new Runs();

        $expected = ['enabled' => false];

        self::assertSame($expected, $runs->toArray());
    }

    public function testToArrayWithFilter(): void
    {
        $filter = new RunsFilter(dateSince: '2023-01-01', limit: 100);
        $runs = new Runs(enabled: true, filter: $filter);

        $expected = [
            'enabled' => true,
            'filter' => [
                'date_since' => '2023-01-01',
                'limit' => 100,
            ],
        ];

        self::assertSame($expected, $runs->toArray());
    }

    public function testToArrayWithEmptyFilter(): void
    {
        $runs = new Runs(enabled: true);

        $expected = ['enabled' => true];

        self::assertSame($expected, $runs->toArray());
    }
}
