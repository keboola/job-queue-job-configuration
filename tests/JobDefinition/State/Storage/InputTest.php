<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\File;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FileTag;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\Table;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\TablesList;
use PHPUnit\Framework\TestCase;

class InputTest extends TestCase
{
    public function testInputConstructor(): void
    {
        $files = new FilesList([
            new File([new FileTag('foo')], '123'),
            new File([new FileTag('bar')], '456'),
        ]);
        $tables = new TablesList([
            new Table('myTable1', '2022-01-01T00:00:00+00:00'),
            new Table('myTable2', '2022-01-02T00:00:00+00:00'),
        ]);

        $input = new Input($tables, $files);
        $this->assertSame($tables, $input->tables);
        $this->assertSame($files, $input->files);
    }

    public function testInputFromArray(): void
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

        $input = Input::fromArray([
            'tables' => $tables,
            'files' => $files,
        ]);

        $this->assertEquals(TablesList::fromArray($tables), $input->tables);
        $this->assertEquals(FilesList::fromArray($files), $input->files);
    }

    public function testInputToArray(): void
    {
        $files = new FilesList([
            new File([new FileTag('foo')], '123'),
            new File([new FileTag('bar')], '456'),
        ]);
        $tables = new TablesList([
            new Table('myTable1', '2022-01-01T00:00:00+00:00'),
            new Table('myTable2', '2022-01-02T00:00:00+00:00'),
        ]);

        $input = new Input($tables, $files);
        $array = $input->toArray();

        self::assertSame($files->toArray(), $array['files']);
        self::assertSame($tables->toArray(), $array['tables']);
    }
}
