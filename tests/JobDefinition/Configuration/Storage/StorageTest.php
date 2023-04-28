<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\FilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Output;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TableFiles;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use PHPUnit\Framework\TestCase;

class StorageTest extends TestCase
{
    public function testFromArray(): void
    {
        $input = Input::fromArray([
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
        ]);

        $output = Output::fromArray([
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
            'table_files' => [
                [
                    'source' => 'in.c-main.test',
                    'destination' => 'test.csv',
                    'tags' => ['my-tag'],
                ],
            ],
            'default_bucket' => 'out.c-main',
        ]);

        $storage = new Storage($input, $output);

        self::assertSame($input, $storage->input);
        self::assertSame($output, $storage->output);
    }

    public function testToArray(): void
    {
        $input = new Input(
            tables: TablesList::fromArray(['table1', 'table2']),
            files: FilesList::fromArray(['file1.csv', 'file2.csv']),
            readOnlyStorageAccess: true
        );

        $output = new Output(
            tables: TablesList::fromArray(['table3', 'table4']),
            files: FilesList::fromArray(['file3.csv', 'file4.csv']),
            tableFiles: TableFiles::fromArray(['file1.csv' => ['table1'], 'file2.csv' => ['table2']]),
            defaultBucket: 'output-bucket'
        );

        $storage = new Storage($input, $output);

        self::assertSame([
            'input' => $input->toArray(),
            'output' => $output->toArray(),
        ], $storage->toArray());
    }
}
