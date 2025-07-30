<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Artifacts;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\RunsFilter;
use PHPUnit\Framework\TestCase;

class RunsFilterTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $filter = new RunsFilter();

        self::assertNull($filter->dateSince);
        self::assertNull($filter->limit);
    }

    public function testConstructorWithParameters(): void
    {
        $filter = new RunsFilter(
            dateSince: '2023-01-01',
            limit: 100,
        );

        self::assertSame('2023-01-01', $filter->dateSince);
        self::assertSame(100, $filter->limit);
    }

    public function testFromEmptyArray(): void
    {
        $filter = RunsFilter::fromArray([]);

        self::assertNull($filter->dateSince);
        self::assertNull($filter->limit);
    }

    public function testFromArray(): void
    {
        $data = [
            'date_since' => '2023-01-01',
            'limit' => 100,
        ];
        $filter = RunsFilter::fromArray($data);

        self::assertSame('2023-01-01', $filter->dateSince);
        self::assertSame(100, $filter->limit);
    }

    public function testFromArrayPartial(): void
    {
        $data = ['date_since' => '2023-01-01'];
        $filter = RunsFilter::fromArray($data);

        self::assertSame('2023-01-01', $filter->dateSince);
        self::assertNull($filter->limit);
    }

    public function testToArrayEmpty(): void
    {
        $filter = new RunsFilter();

        self::assertSame([], $filter->toArray());
    }

    public function testToArray(): void
    {
        $filter = new RunsFilter(
            dateSince: '2023-01-01',
            limit: 100,
        );

        $expected = [
            'date_since' => '2023-01-01',
            'limit' => 100,
        ];

        self::assertSame($expected, $filter->toArray());
    }

    public function testToArrayPartial(): void
    {
        $filter = new RunsFilter(dateSince: '2023-01-01');

        $expected = ['date_since' => '2023-01-01'];

        self::assertSame($expected, $filter->toArray());
    }
}
