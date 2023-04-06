<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\State\Storage;

use Keboola\JobQueue\JobConfiguration\State\Storage\Files\File;
use Keboola\JobQueue\JobConfiguration\State\Storage\Files\FilesList;
use Keboola\JobQueue\JobConfiguration\State\Storage\Files\FileTag;
use Keboola\JobQueue\JobConfiguration\State\Storage\Output;
use Keboola\JobQueue\JobConfiguration\State\Storage\Tables\Table;
use Keboola\JobQueue\JobConfiguration\State\Storage\Tables\TablesList;
use PHPUnit\Framework\TestCase;

class OutputTest extends TestCase
{
    public function testOutputConstructor(): void
    {
        $files = new FilesList([
            new File([new FileTag('foo')], '123'),
            new File([new FileTag('bar')], '456'),
        ]);
        $tables = new TablesList([
            new Table('myTable1', '2022-01-01T00:00:00+00:00'),
            new Table('myTable2', '2022-01-02T00:00:00+00:00'),
        ]);

        $input = new Output($tables, $files);
        $this->assertSame($tables, $input->tables);
        $this->assertSame($files, $input->files);
    }

    public function testOutputFromArray(): void
    {
        $tables = [
            [
                'source' => 'myTable1',
                'lastImportDate' => '2022-01-01T00:00:00+00:00',
            ],
            [
                'source' => 'myTable2',
                'lastImportDate' => '2022-01-02T00:00:00+00:00',
            ],
        ];

        $files = [
            [
                'tags' => [
                    ['name' => 'foo'],
                ],
                'lastImportId' => '123',
            ],
        ];

        $input = Output::fromArray([
            'tables' => $tables,
            'files' => $files,
        ]);

        $this->assertEquals(TablesList::fromArray($tables), $input->tables);
        $this->assertEquals(FilesList::fromArray($files), $input->files);
    }

    public function testOutputToArray(): void
    {
        $files = new FilesList([
            new File([new FileTag('foo')], '123'),
            new File([new FileTag('bar')], '456'),
        ]);
        $tables = new TablesList([
            new Table('myTable1', '2022-01-01T00:00:00+00:00'),
            new Table('myTable2', '2022-01-02T00:00:00+00:00'),
        ]);

        $input = new Output($tables, $files);
        $array = $input->toArray();

        self::assertSame($files->toArray(), $array['files']);
        self::assertSame($tables->toArray(), $array['tables']);
    }
}
