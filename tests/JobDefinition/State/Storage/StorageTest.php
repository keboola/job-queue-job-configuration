<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\File;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FileTag;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\Table;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\TablesList;
use PHPUnit\Framework\TestCase;

class StorageTest extends TestCase
{
    public function testConstructor(): void
    {
        $input = new Input(
            new TablesList([
                new Table('in-myTable1', '2022-01-01T00:00:00+00:00'),
                new Table('in-myTable2', '2022-01-02T00:00:00+00:00'),
            ]),
            new FilesList([
                new File([new FileTag('in-foo')], '123'),
                new File([new FileTag('in-bar')], '456'),
            ]),
        );

        $storage = new Storage($input);

        self::assertSame($input, $storage->input);
    }

    public function testFromArray(): void
    {
        $input = [
            'tables' => [
                [
                    'source' => 'in-myTable1',
                    'lastImportDate' => '2022-01-01T00:00:00+00:00',
                ],
                [
                    'source' => 'in-myTable2',
                    'lastImportDate' => '2022-01-02T00:00:00+00:00',
                ],
            ],
            'files' => [
                [
                    'tags' => [
                        ['name' => 'in-foo'],
                    ],
                    'lastImportId' => '123',
                ],
            ],
        ];

        $output = [
            'tables' => [
                [
                    'source' => 'out-myTable1',
                    'lastImportDate' => '2022-01-01T00:00:00+00:00',
                ],
                [
                    'source' => 'out-myTable2',
                    'lastImportDate' => '2022-01-02T00:00:00+00:00',
                ],
            ],
            'files' => [
                [
                    'tags' => [
                        ['name' => 'out-foo'],
                    ],
                    'lastImportId' => '123',
                ],
            ],
        ];

        $storage = Storage::fromArray([
            'input' => $input,
            'output' => $output,
        ]);

        self::assertEquals(Input::fromArray($input), $storage->input);
    }

    public function testToArray(): void
    {
        $input = new Input(
            new TablesList([
                new Table('in-myTable1', '2022-01-01T00:00:00+00:00'),
                new Table('in-myTable2', '2022-01-02T00:00:00+00:00'),
            ]),
            new FilesList([
                new File([new FileTag('in-foo')], '123'),
                new File([new FileTag('in-bar')], '456'),
            ]),
        );

        $storage = new Storage($input);
        $array = $storage->toArray();

        self::assertSame($input->toArray(), $array['input']);
    }
}
