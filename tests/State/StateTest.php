<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\State;

use Keboola\JobQueue\JobConfiguration\State\State;
use Keboola\JobQueue\JobConfiguration\State\Storage\Files\File;
use Keboola\JobQueue\JobConfiguration\State\Storage\Files\FilesList;
use Keboola\JobQueue\JobConfiguration\State\Storage\Files\FileTag;
use Keboola\JobQueue\JobConfiguration\State\Storage\Input;
use Keboola\JobQueue\JobConfiguration\State\Storage\Output;
use Keboola\JobQueue\JobConfiguration\State\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\State\Storage\Tables\Table;
use Keboola\JobQueue\JobConfiguration\State\Storage\Tables\TablesList;
use PHPUnit\Framework\TestCase;

class StateTest extends TestCase
{
    public function testConstructor(): void
    {
        $storage = new Storage(
            new Input(
                new TablesList([
                    new Table('in-myTable1', '2022-01-01T00:00:00+00:00'),
                    new Table('in-myTable2', '2022-01-02T00:00:00+00:00'),
                ]),
                new FilesList([
                    new File([new FileTag('in-foo')], '123'),
                    new File([new FileTag('in-bar')], '456'),
                ]),
            ),
            new Output(
                new TablesList([
                    new Table('out-myTable1', '2022-01-01T00:00:00+00:00'),
                    new Table('out-myTable2', '2022-01-02T00:00:00+00:00'),
                ]),
                new FilesList([
                    new File([new FileTag('out-foo')], '123'),
                    new File([new FileTag('out-bar')], '456'),
                ]),
            ),
        );

        $state = new State($storage);

        self::assertSame($storage, $state->storage);
    }

    public function testFromArray(): void
    {
        $storage = [
            'input' => [
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
            ],

            'output' => [
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
            ],
        ];

        $state = State::fromArray([
            'storage' => $storage,
        ]);

        self::assertEquals(Storage::fromArray($storage), $state->storage);
    }

    public function testToArray(): void
    {
        $storage = new Storage(
            new Input(
                new TablesList([
                    new Table('in-myTable1', '2022-01-01T00:00:00+00:00'),
                    new Table('in-myTable2', '2022-01-02T00:00:00+00:00'),
                ]),
                new FilesList([
                    new File([new FileTag('in-foo')], '123'),
                    new File([new FileTag('in-bar')], '456'),
                ]),
            ),
            new Output(
                new TablesList([
                    new Table('out-myTable1', '2022-01-01T00:00:00+00:00'),
                    new Table('out-myTable2', '2022-01-02T00:00:00+00:00'),
                ]),
                new FilesList([
                    new File([new FileTag('out-foo')], '123'),
                    new File([new FileTag('out-bar')], '456'),
                ]),
            ),
        );

        $state = new State($storage);
        $array = $state->toArray();

        self::assertSame($storage->toArray(), $array['storage']);
    }
}
