<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\FilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Output;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TableFilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use PHPUnit\Framework\TestCase;

class OutputTest extends TestCase
{
    public function testFromArray(): void
    {
        $data = [
            'tables' => [
                [
                    'source' => 'my-table.csv',
                    'destination' => 'out.c-my-bucket.my-table',
                    'metadata' => [
                        'key' => 'value',
                    ],
                ],
            ],
            'files' => [
                [
                    'tags' => ['my-tag'],
                    'path' => '/my-file.csv',
                ],
            ],
            'table_files' => [
                [
                    'tableId' => 'in.c-my-bucket.my-table',
                    'tags' => ['my-tag'],
                    'path' => '/my-file.csv',
                ],
            ],
            'default_bucket' => 'out.c-my-bucket',
        ];

        $output = Output::fromArray($data);

        self::assertSame('out.c-my-bucket', $output->defaultBucket);

        self::assertEquals(TablesList::fromArray([
            [
                'source' => 'my-table.csv',
                'destination' => 'out.c-my-bucket.my-table',
                'metadata' => [
                    'key' => 'value',
                ],
            ],
        ]), $output->tables);

        self::assertEquals(FilesList::fromArray([
            [
                'tags' => ['my-tag'],
                'path' => '/my-file.csv',
            ],
        ]), $output->files);

        self::assertEquals(TableFilesList::fromArray([
            [
                'tableId' => 'in.c-my-bucket.my-table',
                'tags' => ['my-tag'],
                'path' => '/my-file.csv',
            ],
        ]), $output->tableFiles);
    }

    public function testToArray(): void
    {
        $tablesList = TablesList::fromArray([
            [
                'source' => 'my-table.csv',
                'destination' => 'out.c-my-bucket.my-table',
                'metadata' => [
                    'key' => 'value',
                ],
            ],
        ]);
        $filesList = FilesList::fromArray([
            [
                'tags' => ['my-tag'],
                'path' => '/my-file.csv',
            ],
        ]);
        $tableFilesList = TableFilesList::fromArray([
            [
                'tableId' => 'in.c-my-bucket.my-table',
                'tags' => ['my-tag'],
                'path' => '/my-file.csv',
            ],
        ]);

        $output = new Output(
            tables: $tablesList,
            files: $filesList,
            tableFiles: $tableFilesList,
            defaultBucket: 'out.c-my-bucket'
        );

        $data = $output->toArray();

        self::assertSame([
            'tables' => $tablesList->toArray(),
            'files' => $filesList->toArray(),
            'table_files' => $tableFilesList->toArray(),
            'default_bucket' => 'out.c-my-bucket',
        ], $data);
    }

    public function testIsEmpty(): void
    {
        $output = new Output(
            tables: new TablesList([]),
            files: new FilesList([]),
            tableFiles: new TableFilesList([]),
            defaultBucket: 'out.c-my-bucket'
        );

        self::assertTrue($output->isEmpty());

        $output = new Output(
            tables: new TablesList([
                [
                    'source' => 'my-table.csv',
                    'destination' => 'out.c-my-bucket.my-table',
                    'metadata' => [
                        'key' => 'value',
                    ],
                ],
            ]),
            files: new FilesList([]),
            tableFiles: new TableFilesList([]),
            defaultBucket: 'out.c-my-bucket'
        );

        self::assertFalse($output->isEmpty());

        $output = new Output(
            tables: new TablesList([]),
            files: new FilesList([
                [
                    'tags' => ['my-tag'],
                    'path' => '/my-file.csv',
                ],
            ]),
            tableFiles: new TableFilesList([]),
            defaultBucket: 'out.c-my-bucket'
        );

        self::assertFalse($output->isEmpty());

        $output = new Output(
            tables: new TablesList([]),
            files: new FilesList([]),
            tableFiles: new TableFilesList([
                [
                    'tableId' => 'in.c-my-bucket.my-table',
                    'tags' => ['my-tag'],
                    'path' => '/my-file.csv',
                ],
            ]),
            defaultBucket: 'out.c-my-bucket'
        );

        self::assertFalse($output->isEmpty());
    }
}
