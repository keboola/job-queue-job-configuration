<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Artifacts;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Custom;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\CustomFilter;
use PHPUnit\Framework\TestCase;

class CustomTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $custom = new Custom();

        self::assertFalse($custom->enabled);
        self::assertEquals(new CustomFilter(), $custom->filter);
    }

    public function testConstructorWithParameters(): void
    {
        $filter = new CustomFilter(
            componentId: 'keboola.ex-db',
            configId: '123',
            branchId: '456',
        );
        $custom = new Custom(enabled: true, filter: $filter);

        self::assertTrue($custom->enabled);
        self::assertEquals($filter, $custom->filter);
    }

    public function testFromEmptyArray(): void
    {
        $custom = Custom::fromArray([]);

        self::assertFalse($custom->enabled);
        self::assertEquals(new CustomFilter(), $custom->filter);
    }

    public function testFromArray(): void
    {
        $data = [
            'enabled' => true,
            'filter' => [
                'component_id' => 'keboola.ex-db',
                'config_id' => '123',
                'branch_id' => '456',
                'date_since' => '2023-01-01',
                'limit' => 100,
            ],
        ];
        $custom = Custom::fromArray($data);

        self::assertTrue($custom->enabled);
        self::assertEquals(CustomFilter::fromArray($data['filter']), $custom->filter);
    }

    public function testFromArrayOnlyEnabled(): void
    {
        $data = ['enabled' => true];
        $custom = Custom::fromArray($data);

        self::assertTrue($custom->enabled);
        self::assertEquals(new CustomFilter(), $custom->filter);
    }

    public function testToArrayDefaultValues(): void
    {
        $custom = new Custom();

        $expected = ['enabled' => false];

        self::assertSame($expected, $custom->toArray());
    }

    public function testToArrayWithFilter(): void
    {
        $filter = new CustomFilter(
            componentId: 'keboola.ex-db',
            configId: '123',
            branchId: '456',
            dateSince: '2023-01-01',
            limit: 100,
        );
        $custom = new Custom(enabled: true, filter: $filter);

        $expected = [
            'enabled' => true,
            'filter' => [
                'component_id' => 'keboola.ex-db',
                'config_id' => '123',
                'branch_id' => '456',
                'date_since' => '2023-01-01',
                'limit' => 100,
            ],
        ];

        self::assertSame($expected, $custom->toArray());
    }

    public function testToArrayWithEmptyFilter(): void
    {
        $custom = new Custom(enabled: true);

        $expected = ['enabled' => true];

        self::assertSame($expected, $custom->toArray());
    }
}
