<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Artifacts;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\CustomFilter;
use PHPUnit\Framework\TestCase;

class CustomFilterTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $filter = new CustomFilter();

        self::assertNull($filter->componentId);
        self::assertNull($filter->configId);
        self::assertNull($filter->branchId);
        self::assertNull($filter->dateSince);
        self::assertNull($filter->limit);
    }

    public function testConstructorWithParameters(): void
    {
        $filter = new CustomFilter(
            componentId: 'keboola.ex-db',
            configId: '123',
            branchId: '456',
            dateSince: '2023-01-01',
            limit: 100,
        );

        self::assertSame('keboola.ex-db', $filter->componentId);
        self::assertSame('123', $filter->configId);
        self::assertSame('456', $filter->branchId);
        self::assertSame('2023-01-01', $filter->dateSince);
        self::assertSame(100, $filter->limit);
    }

    public function testFromEmptyArray(): void
    {
        $filter = CustomFilter::fromArray([]);

        self::assertNull($filter->componentId);
        self::assertNull($filter->configId);
        self::assertNull($filter->branchId);
        self::assertNull($filter->dateSince);
        self::assertNull($filter->limit);
    }

    public function testFromArray(): void
    {
        $data = [
            'component_id' => 'keboola.ex-db',
            'config_id' => '123',
            'branch_id' => '456',
            'date_since' => '2023-01-01',
            'limit' => 100,
        ];
        $filter = CustomFilter::fromArray($data);

        self::assertSame('keboola.ex-db', $filter->componentId);
        self::assertSame('123', $filter->configId);
        self::assertSame('456', $filter->branchId);
        self::assertSame('2023-01-01', $filter->dateSince);
        self::assertSame(100, $filter->limit);
    }

    public function testFromArrayPartial(): void
    {
        $data = [
            'component_id' => 'keboola.ex-db',
            'config_id' => '123',
        ];
        $filter = CustomFilter::fromArray($data);

        self::assertSame('keboola.ex-db', $filter->componentId);
        self::assertSame('123', $filter->configId);
        self::assertNull($filter->branchId);
        self::assertNull($filter->dateSince);
        self::assertNull($filter->limit);
    }

    public function testToArrayEmpty(): void
    {
        $filter = new CustomFilter();

        self::assertSame([], $filter->toArray());
    }

    public function testToArray(): void
    {
        $filter = new CustomFilter(
            componentId: 'keboola.ex-db',
            configId: '123',
            branchId: '456',
            dateSince: '2023-01-01',
            limit: 100,
        );

        $expected = [
            'component_id' => 'keboola.ex-db',
            'config_id' => '123',
            'branch_id' => '456',
            'date_since' => '2023-01-01',
            'limit' => 100,
        ];

        self::assertSame($expected, $filter->toArray());
    }

    public function testToArrayPartial(): void
    {
        $filter = new CustomFilter(
            componentId: 'keboola.ex-db',
            configId: '123',
        );

        $expected = [
            'component_id' => 'keboola.ex-db',
            'config_id' => '123',
        ];

        self::assertSame($expected, $filter->toArray());
    }
}
