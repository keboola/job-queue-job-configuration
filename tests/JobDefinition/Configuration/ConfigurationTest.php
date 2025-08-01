<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration;

use Keboola\JobQueue\JobConfiguration\Exception\InvalidDataException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Artifacts;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\Processor;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\ProcessorDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\Processors;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Backend;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Runtime;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Output;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\TableModifications;
use PHPUnit\Framework\TestCase;

class ConfigurationTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $configuration = new Configuration();

        self::assertSame([], $configuration->parameters);
        self::assertEquals(new Storage(), $configuration->storage);
        self::assertEquals(new Processors(), $configuration->processors);
        self::assertNull($configuration->runtime);
        self::assertNull($configuration->variablesId);
        self::assertNull($configuration->variablesValuesId);
        self::assertNull($configuration->sharedCodeId);
        self::assertSame([], $configuration->sharedCodeRowIds);
        self::assertNull($configuration->imageParameters);
        self::assertSame([], $configuration->authorization);
        self::assertNull($configuration->action);
        self::assertEquals(new Artifacts(), $configuration->artifacts);
    }

    public function testFromEmptyArray(): void
    {
        $configuration = Configuration::fromArray([]);

        self::assertSame([], $configuration->parameters);
        self::assertEquals(new Storage(), $configuration->storage);
        self::assertEquals(new Processors(), $configuration->processors);
        self::assertNull($configuration->runtime);
        self::assertNull($configuration->variablesId);
        self::assertNull($configuration->variablesValuesId);
        self::assertNull($configuration->sharedCodeId);
        self::assertSame([], $configuration->sharedCodeRowIds);
        self::assertNull($configuration->imageParameters);
        self::assertSame([], $configuration->authorization);
        self::assertNull($configuration->action);
        self::assertEquals(new Artifacts(), $configuration->artifacts);
    }

    public function testFromArray(): void
    {
        $parameters = [
            'foo' => 'bar',
        ];

        $storage = [
            'input' => [
                'tables' => [
                    [
                        'source' => 'in.c-main.test',
                        'destination' => 'test.csv',
                        'columns' => [],
                        'column_types' => [],
                        'where_values' => [],
                        'where_operator' => 'eq',
                        'overwrite' => false,
                        'use_view' => false,
                        'keep_internal_timestamp_column' => true,
                    ],
                ],
            ],
        ];

        $processors = [
            'after' => [
                [
                    'definition' => [
                        'component' => 'foo',
                    ],
                ],
            ],
            'before' => [
                [
                    'definition' => [
                        'component' => 'bar',
                    ],
                ],
            ],
        ];

        $runtime = [
            'backend' => [
                'type' => 'snowflake',
            ],
        ];

        $configuration = Configuration::fromArray([
            'parameters' => $parameters,
            'storage' => $storage,
            'processors' => $processors,
            'runtime' => $runtime,
            'variables_id' => '123',
            'variables_values_id' => '456',
            'shared_code_id' => '789',
            'shared_code_row_ids' => ['foo', 'bar'],
            'image_parameters' => [
                'foo' => 'bar',
            ],
            'authorization' => [
                'oauth_api' => [
                    'credentials' => [
                        'id' => '123',
                    ],
                ],
            ],
            'action' => 'run',
            'artifacts' => [
                'runs' => [
                    'enabled' => true,
                    'filter' => [
                        'limit' => 10,
                    ],
                ],
            ],
        ]);

        self::assertSame($parameters, $configuration->parameters);
        self::assertEquals(Storage::fromArray($storage), $configuration->storage);
        self::assertEquals(Processors::fromArray($processors), $configuration->processors);
        self::assertEquals(Runtime::fromArray($runtime), $configuration->runtime);
        self::assertSame('123', $configuration->variablesId);
        self::assertSame('456', $configuration->variablesValuesId);
        self::assertSame('789', $configuration->sharedCodeId);
        self::assertSame(['foo', 'bar'], $configuration->sharedCodeRowIds);
        self::assertSame(['foo' => 'bar'], $configuration->imageParameters);
        self::assertSame(
            [
                'oauth_api' => [
                    'credentials' => [
                        'id' => '123',
                    ],
                ],
            ],
            $configuration->authorization,
        );
        self::assertSame('run', $configuration->action);
        self::assertEquals(
            Artifacts::fromArray([
                'runs' => [
                    'enabled' => true,
                    'filter' => [
                        'limit' => 10,
                    ],
                ],
            ]),
            $configuration->artifacts,
        );
    }

    public function testFromArrayWithInvalidData(): void
    {
        try {
            Configuration::fromArray([
                'foo' => 'bar',
            ]);

            self::fail('Expected exception');
        } catch (InvalidDataException $e) {
            self::assertStringContainsString(
                'Job configuration data is not valid: Unrecognized option "foo" under "configuration". ',
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

    public static function provideToArrayData(): iterable
    {
        yield 'minimal config' => [
            'config' => new Configuration(),
            'output' => [
                'action' => null,
                'parameters' => [],
                'storage' => [
                    'input' => [
                        'tables' => [],
                        'files' => [],
                        'read_only_storage_access' => null,
                    ],
                    'output' => [
                        'tables' => [],
                        'files' => [],
                        'table_files' => [
                            'tags' => [],
                            'is_permanent' => true,
                        ],
                        'default_bucket' => null,
                        'table_modifications' => null,
                        'treat_values_as_null' => null,
                    ],
                ],
                'processors' => [],
                'artifacts' => [
                    'options' => ['zip' => true],
                    'runs' => ['enabled' => false],
                    'custom' => ['enabled' => false],
                    'shared' => ['enabled' => false],
                ],
            ],
        ];

        yield 'full config' => [
            'config' => new Configuration(
                parameters: [
                    'foo' => 'bar',
                ],
                storage: new Storage(
                    input: new Input(
                        tables: new TablesList([
                            [
                                'source' => 'in.c-main.test',
                                'destination' => 'test.csv',
                            ],
                        ]),
                    ),
                    output: new Output(
                        tableModifications: TableModifications::NON_DESTRUCTIVE,
                        treatValuesAsNull: ['null', 'NAN'],
                    ),
                ),
                processors: new Processors(
                    before: [
                        new Processor(
                            definition: new ProcessorDefinition(
                                component: 'bar',
                            ),
                        ),
                    ],
                    after: [
                        new Processor(
                            definition: new ProcessorDefinition(
                                component: 'foo',
                            ),
                        ),
                    ],
                ),
                runtime: new Runtime(
                    backend: new Backend(
                        type: 'small',
                    ),
                ),
                variablesId: '123',
                variablesValuesId: '456',
                sharedCodeId: '789',
                sharedCodeRowIds: ['foo', 'bar'],
                imageParameters: [
                    'foo' => 'bar',
                ],
                authorization: [
                    'oauth_api' => [
                        'credentials' => [
                            'id' => '123',
                        ],
                    ],
                ],
                action: 'run',
                artifacts: Artifacts::fromArray([
                    'runs' => [
                        'enabled' => true,
                        'filter' => [
                            'limit' => 10,
                        ],
                    ],
                ]),
            ),
            'output' => [
                'action' => 'run',
                'parameters' => [
                    'foo' => 'bar',
                ],
                'storage' => [
                    'input' => [
                        'tables' => [
                            [
                                'source' => 'in.c-main.test',
                                'destination' => 'test.csv',
                            ],
                        ],
                        'files' => [],
                        'read_only_storage_access' => null,
                    ],
                    'output' => [
                        'tables' => [],
                        'files' => [],
                        'table_files' => [
                            'tags' => [],
                            'is_permanent' => true,
                        ],
                        'default_bucket' => null,
                        'table_modifications' => 'non-destructive',
                        'treat_values_as_null' => ['null', 'NAN'],
                    ],
                ],
                'processors' => [
                    'before' => [
                        [
                            'definition' => [
                                'component' => 'bar',
                            ],
                        ],
                    ],
                    'after' => [
                        [
                            'definition' => [
                                'component' => 'foo',
                            ],
                        ],
                    ],
                ],
                'artifacts' => [
                    'options' => ['zip' => true],
                    'runs' => [
                        'enabled' => true,
                        'filter' => [
                            'limit' => 10,
                        ],
                    ],
                    'custom' => ['enabled' => false],
                    'shared' => ['enabled' => false],
                ],
                'runtime' => [
                    'safe' => null,
                    'image_tag' => null,
                    'process_timeout' => null,
                    'use_file_storage_only' => null,
                    'backend' => [
                        'type' => 'small',
                        'context' => null,
                        'workspace_credentials' => null,
                    ],
                ],
                'variables_id' => '123',
                'variables_values_id' => '456',
                'shared_code_id' => '789',
                'shared_code_row_ids' => ['foo', 'bar'],
                'image_parameters' => [
                    'foo' => 'bar',
                ],
                'authorization' => [
                    'oauth_api' => [
                        'credentials' => [
                            'id' => '123',
                        ],
                    ],
                ],
            ],
        ];
    }

    /** @dataProvider provideToArrayData */
    public function testToArray(Configuration $configuration, array $output): void
    {
        self::assertSame($output, $configuration->toArray());
    }

    public function testMergeArray(): void
    {
        $configuration = Configuration::fromArray([
            'parameters' => [
                'foo' => 'bar',
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.test',
                            'destination' => 'test.csv',
                        ],
                    ],
                ],
            ],
            'processors' => [
                'after' => [
                    [
                        'definition' => [
                            'component' => 'foo',
                        ],
                    ],
                ],
            ],
            'runtime' => [
                'backend' => [
                    'type' => 'snowflake',
                ],
            ],
        ]);

        $mergedConfiguration = $configuration->mergeArray([
            'parameters' => [
                'foo' => 'baz',
                'faa' => 'xxx',
            ],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'destination' => 'custom.csv',
                        ],
                    ],
                ],
            ],
            'processors' => [
                'before' => [
                    [
                        'definition' => [
                            'component' => 'bar',
                        ],
                    ],
                ],
            ],
            'runtime' => [
                'backend' => [
                    'type' => 'bigquery',
                ],
            ],
        ]);

        self::assertSame([
            'foo' => 'baz',
            'faa' => 'xxx',
        ], $mergedConfiguration->parameters);
        self::assertSame([
            'before' => [
                [
                    'definition' => [
                        'component' => 'bar',
                    ],
                ],
            ],
            'after' => [
                [
                    'definition' => [
                        'component' => 'foo',
                    ],
                ],
            ],
        ], $mergedConfiguration->processors->toArray());
        self::assertSame('bigquery', $mergedConfiguration->runtime?->backend?->type);
    }
}
