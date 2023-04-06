<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Configuration\Storage;

use Keboola\JobQueue\JobConfiguration\Configuration\Storage\TablesList;
use PHPUnit\Framework\TestCase;

class TablesListTest extends TestCase
{
    public function testFromArray(): void
    {
        $tablesList = TablesList::fromArray([
            [
                'dbName' => 'database1',
                'tableName' => 'table1',
            ],
            [
                'dbName' => 'database2',
                'tableName' => 'table2',
            ],
        ]);

        self::assertCount(2, $tablesList);
        self::assertFalse($tablesList->isEmpty());
        self::assertSame([
            [
                'dbName' => 'database1',
                'tableName' => 'table1',
            ],
            [
                'dbName' => 'database2',
                'tableName' => 'table2',
            ],
        ], $tablesList->toArray());
    }

    public function testToArray(): void
    {
        $tablesList = new TablesList([
            [
                'dbName' => 'database1',
                'tableName' => 'table1',
            ],
            [
                'dbName' => 'database2',
                'tableName' => 'table2',
            ],
        ]);

        self::assertEquals([
            [
                'dbName' => 'database1',
                'tableName' => 'table1',
            ],
            [
                'dbName' => 'database2',
                'tableName' => 'table2',
            ],
        ], $tablesList->toArray());
    }
}
