<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\JobQueue\JobConfiguration\Exception\InvalidDataException;
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
        self::assertNull($state->dataApp);
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

        $dataApp = [
            'config' => 'value',
        ];

        $state = new State($storage, $component, $dataApp);

        self::assertSame($storage, $state->storage);
        self::assertSame($component, $state->component);
        self::assertSame($dataApp, $state->dataApp);
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

        $dataApp = [
            'config' => 'value',
        ];

        $state = State::fromArray([
            'storage' => $storage,
            'component' => $component,
            'data_app' => $dataApp,
        ]);

        self::assertEquals(Storage::fromArray($storage), $state->storage);
        self::assertSame($component, $state->component);
        self::assertSame($dataApp, $state->dataApp);
    }

    public function testFromArrayWithInvalidData(): void
    {
        try {
            State::fromArray([
                'foo' => 'bar',
            ]);

            self::fail('Expected exception');
        } catch (InvalidDataException $e) {
            self::assertStringContainsString(
                'Job state data is not valid: Unrecognized option "foo" under "state". ',
                $e->getMessage(),
            );
            self::assertSame(
                [
                    'foo' => 'bar',
                ],
                $e->getContext(),
            );
        }
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

        $dataApp = [
            'config' => 'value',
        ];

        $state = new State($storage, $component, $dataApp);
        $array = $state->toArray();

        self::assertSame($storage->toArray(), $array['storage']);
        self::assertSame($component, $array['component']);
        self::assertSame($dataApp, $array['data_app']);
    }

    public function testToArrayWithoutDataApp(): void
    {
        $storage = new Storage(
            new Input(
                new TablesList([
                    new Table('in-myTable1', '2022-01-01T00:00:00+00:00'),
                ]),
                new FilesList([]),
            ),
        );

        $component = [
            'foo' => 'bar',
        ];

        $state = new State($storage, $component);
        $array = $state->toArray();

        self::assertSame($storage->toArray(), $array['storage']);
        self::assertSame($component, $array['component']);
        self::assertArrayNotHasKey('data_app', $array);
    }

    public function testWithStorage(): void
    {
        $initialStorage = new Storage();
        $component = ['foo' => 'bar'];
        $dataApp = ['config' => 'value'];
        $state = new State($initialStorage, $component, $dataApp);

        $newStorage = new Storage(
            new Input(
                new TablesList([
                    new Table('in-myTable1', '2022-01-01T00:00:00+00:00'),
                ]),
                new FilesList([]),
            ),
        );

        $newState = $state->withStorage($newStorage);

        self::assertSame($newStorage, $newState->storage);
        self::assertSame($component, $newState->component);
        self::assertSame($dataApp, $newState->dataApp);
        self::assertSame($initialStorage, $state->storage);
        self::assertNotSame($state, $newState);
    }

    public function testWithComponent(): void
    {
        $storage = new Storage();
        $initialComponent = ['foo' => 'bar'];
        $dataApp = ['config' => 'value'];
        $state = new State($storage, $initialComponent, $dataApp);

        $newComponent = ['baz' => 'qux'];
        $newState = $state->withComponent($newComponent);

        self::assertSame($newComponent, $newState->component);
        self::assertSame($storage, $newState->storage);
        self::assertSame($dataApp, $newState->dataApp);
        self::assertSame($initialComponent, $state->component);
        self::assertNotSame($state, $newState);
    }

    public function testWithDataApp(): void
    {
        $storage = new Storage();
        $component = ['foo' => 'bar'];
        $initialDataApp = ['config' => 'value'];
        $state = new State($storage, $component, $initialDataApp);

        $newDataApp = ['config' => 'new-value'];
        $newState = $state->withDataApp($newDataApp);

        self::assertSame($newDataApp, $newState->dataApp);
        self::assertSame($storage, $newState->storage);
        self::assertSame($component, $newState->component);
        self::assertSame($initialDataApp, $state->dataApp);
        self::assertNotSame($state, $newState);
    }

    public function testWithDataAppNull(): void
    {
        $storage = new Storage();
        $component = ['foo' => 'bar'];
        $initialDataApp = ['config' => 'value'];
        $state = new State($storage, $component, $initialDataApp);

        $newState = $state->withDataApp(null);

        self::assertNull($newState->dataApp);
        self::assertSame($storage, $newState->storage);
        self::assertSame($component, $newState->component);
        self::assertSame($initialDataApp, $state->dataApp);
        self::assertNotSame($state, $newState);
    }
}
