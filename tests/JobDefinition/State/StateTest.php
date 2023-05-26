<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\File;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FileTag;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\Table;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\TablesList;
use PHPUnit\Framework\TestCase;

class StateTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $state = new State();

        self::assertEquals(new Storage(), $state->storage);
        self::assertSame([], $state->component);
    }

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
        );

        $component = [
            'foo' => 'bar',
        ];

        $state = new State($storage, $component);

        self::assertSame($storage, $state->storage);
        self::assertSame($component, $state->component);
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
        ];

        $component = [
            'foo' => 'bar',
        ];

        $state = State::fromArray([
            'storage' => $storage,
            'component' => $component,
        ]);

        self::assertEquals(Storage::fromArray($storage), $state->storage);
    }

    public function testFromArrayWithInvalidData(): void
    {
        $this->expectException(ApplicationExceptionInterface::class);
        $this->expectExceptionMessage(
            'Job state is not valid: Unrecognized option "foo" under "state". Available options are'
        );

        State::fromArray([
            'foo' => 'bar',
        ]);
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
        );

        $component = [
            'foo' => 'bar',
        ];

        $state = new State($storage, $component);
        $array = $state->toArray();

        self::assertSame($storage->toArray(), $array['storage']);
        self::assertSame($component, $array['component']);
    }
}
