<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Configuration\Storage;

use Keboola\JobQueue\JobConfiguration\Configuration\Storage\TableFilesList;
use PHPUnit\Framework\TestCase;

class TableFilesListTest extends TestCase
{
    public function testFromArray(): void
    {
        $data = [
            ['tableId' => 'in.c-main.customers', 'source' => 'customers.csv', 'destination' => 'customers_output.csv'],
            ['tableId' => 'in.c-main.orders', 'source' => 'orders.csv', 'destination' => 'orders_output.csv'],
        ];

        $filesList = TableFilesList::fromArray($data);

        self::assertCount(2, $filesList);
        self::assertSame($data, $filesList->toArray());
    }

    public function testToArray(): void
    {
        $items = [
            ['tableId' => 'in.c-main.customers', 'source' => 'customers.csv', 'destination' => 'customers_output.csv'],
            ['tableId' => 'in.c-main.orders', 'source' => 'orders.csv', 'destination' => 'orders_output.csv'],
        ];

        $tableFiles = new TableFilesList($items);

        self::assertSame($items, $tableFiles->toArray());
    }

    public function testCount(): void
    {
        $tableFiles = new TableFilesList([
            ['tableId' => 'in.c-main.customers', 'source' => 'customers.csv', 'destination' => 'customers_output.csv'],
            ['tableId' => 'in.c-main.orders', 'source' => 'orders.csv', 'destination' => 'orders_output.csv'],
        ]);

        self::assertCount(2, $tableFiles);
    }

    public function testIsEmpty(): void
    {
        $tableFiles = new TableFilesList([]);
        self::assertTrue($tableFiles->isEmpty());

        $tableFiles = new TableFilesList([
            ['tableId' => 'in.c-main.customers', 'source' => 'customers.csv', 'destination' => 'customers_output.csv'],
        ]);
        self::assertFalse($tableFiles->isEmpty());
    }
}
