<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State\Storage\Tables;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\Table;
use PHPUnit\Framework\TestCase;

class TableTest extends TestCase
{
    public function testTableConstructor(): void
    {
        $table = new Table('mySource', '2022-01-01T00:00:00+00:00');

        self::assertSame('mySource', $table->source);
        self::assertSame('2022-01-01T00:00:00+00:00', $table->lastImportDate);
    }

    public function testTableFromArray(): void
    {
        $data = [
            'source' => 'mySource',
            'lastImportDate' => '2022-01-01T00:00:00+00:00',
        ];
        $table = Table::fromArray($data);
        self::assertSame('mySource', $table->source);
        self::assertSame('2022-01-01T00:00:00+00:00', $table->lastImportDate);
    }

    public function testTableToArray(): void
    {
        $table = new Table('mySource', '2022-01-01T00:00:00+00:00');

        $data = $table->toArray();
        self::assertSame([
            'source' => 'mySource',
            'lastImportDate' => '2022-01-01T00:00:00+00:00',
        ], $data);
    }
}
