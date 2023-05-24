<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\FilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use PHPUnit\Framework\TestCase;

class InputTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $input = new Input();

        self::assertEquals(new TablesList(), $input->tables);
        self::assertEquals(new FilesList(), $input->files);
        self::assertNull($input->readOnlyStorageAccess);
    }

    public function testFromArray(): void
    {
        $data = [
            'tables' => [
                [
                    'source' => 'in.c-main.test',
                    'destination' => 'test.csv',
                ],
            ],
            'files' => [
                [
                    'source' => 'in.c-main.file',
                    'destination' => 'test.txt',
                ],
            ],
            'read_only_storage_access' => true,
        ];

        $input = Input::fromArray($data);

        self::assertEquals(TablesList::fromArray([
            [
                'source' => 'in.c-main.test',
                'destination' => 'test.csv',
            ],
        ]), $input->tables);
        self::assertEquals(FilesList::fromArray([
            [
                'source' => 'in.c-main.file',
                'destination' => 'test.txt',
            ],
        ]), $input->files);
        self::assertTrue($input->readOnlyStorageAccess);
    }

    public function testToArray(): void
    {
        $tables = new TablesList([
            [
                'source' => 'in.c-main.test',
                'destination' => 'test.csv',
            ],
        ]);

        $files = new FilesList([
            [
                'source' => 'in.c-main.file',
                'destination' => 'test.txt',
            ],
        ]);

        $input = new Input(
            tables: $tables,
            files: $files,
            readOnlyStorageAccess: true,
        );

        self::assertSame([
            'tables' => $tables->toArray(),
            'files' => $files->toArray(),
            'read_only_storage_access' => true,
        ], $input->toArray());
    }

    public function testIsEmpty(): void
    {
        $input = new Input(
            tables: new TablesList([]),
            files: new FilesList([]),
            readOnlyStorageAccess: null,
        );

        self::assertTrue($input->isEmpty());

        $input = new Input(
            tables: new TablesList([
                [
                    'source' => 'in.c-main.test',
                    'destination' => 'test.csv',
                ],
            ]),
            files: new FilesList([]),
            readOnlyStorageAccess: null,
        );

        self::assertFalse($input->isEmpty());

        $input = new Input(
            tables: new TablesList([]),
            files: new FilesList([
                [
                    'source' => 'in.c-main.file',
                    'destination' => 'test.txt',
                ],
            ]),
            readOnlyStorageAccess: null,
        );

        self::assertFalse($input->isEmpty());
    }
}
