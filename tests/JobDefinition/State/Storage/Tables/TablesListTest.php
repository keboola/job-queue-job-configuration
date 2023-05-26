<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State\Storage\Tables;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\Table;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\TablesList;
use PHPUnit\Framework\TestCase;

class TablesListTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $tablesList = new TablesList();

        self::assertSame([], $tablesList->toArray());
    }

    public function testFromToArray(): void
    {
        $table1 = Table::fromArray([
            'source' => 'in.c-main.source',
            'lastImportDate' => '2022-01-01T00:00:00+00:00',
        ]);

        $table2 = Table::fromArray([
            'source' => 'in.c-main.source2',
            'lastImportDate' => '2022-01-02T00:00:00+00:00',
        ]);

        $tablesList = new TablesList([$table1, $table2]);

        $this->assertSame([
            $table1->toArray(),
            $table2->toArray(),
        ], $tablesList->toArray());
    }
}
