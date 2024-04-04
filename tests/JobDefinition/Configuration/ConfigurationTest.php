<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Backend;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Runtime;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use PHPUnit\Framework\TestCase;

class ConfigurationTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $configuration = new Configuration();

        self::assertSame([], $configuration->parameters);
        self::assertEquals(new Storage(), $configuration->storage);
        self::assertSame([], $configuration->processors);
        self::assertNull($configuration->runtime);
        self::assertNull($configuration->variablesId);
        self::assertNull($configuration->variablesValuesId);
        self::assertNull($configuration->sharedCodeId);
        self::assertSame([], $configuration->sharedCodeRowIds);
        self::assertNull($configuration->imageParameters);
        self::assertSame([], $configuration->authorization);
        self::assertNull($configuration->action);
        self::assertSame([], $configuration->artifacts);
    }

    public function testFromEmptyArray(): void
    {
        $configuration = Configuration::fromArray([]);

        self::assertSame([], $configuration->parameters);
        self::assertEquals(new Storage(), $configuration->storage);
        self::assertSame([], $configuration->processors);
        self::assertNull($configuration->runtime);
        self::assertNull($configuration->variablesId);
        self::assertNull($configuration->variablesValuesId);
        self::assertNull($configuration->sharedCodeId);
        self::assertSame([], $configuration->sharedCodeRowIds);
        self::assertNull($configuration->imageParameters);
        self::assertSame([], $configuration->authorization);
        self::assertNull($configuration->action);
        self::assertSame([], $configuration->artifacts);
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
        self::assertSame($processors, $configuration->processors);
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
        self::assertSame(
            [
                'runs' => [
                    'enabled' => true,
                    'filter' => [
                        'limit' => 10,
                    ],
                ],
            ],
            $configuration->artifacts,
        );
    }

    public function testFromArrayWithInvalidData(): void
    {
        $this->expectException(ApplicationExceptionInterface::class);
        $this->expectExceptionMessage(
            'Job configuration is not valid: Unrecognized option "foo" under "configuration". Available options are',
        );

        Configuration::fromArray([
            'foo' => 'bar',
        ]);
    }

    public function testToArray(): void
    {
        $configuration = new Configuration(
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
            ),
            processors: [
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
            artifacts: [
                'runs' => [
                    'enabled' => true,
                    'filter' => [
                        'limit' => 10,
                    ],
                ],
            ],
        );

        self::assertSame([
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
            'runtime' => [
                'safe' => null,
                'image_tag' => null,
                'use_file_storage_only' => null,
                'backend' => [
                    'type' => 'small',
                    'context' => null,
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
            'action' => 'run',
            'artifacts' => [
                'runs' => [
                    'enabled' => true,
                    'filter' => [
                        'limit' => 10,
                    ],
                ],
            ],
        ], $configuration->toArray());
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
                    'type' => 'redshift',
                ],
            ],
        ]);

        self::assertSame([
            'foo' => 'baz',
            'faa' => 'xxx',
        ], $mergedConfiguration->parameters);
        self::assertSame([
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
        ], $mergedConfiguration->processors);
        self::assertSame('redshift', $mergedConfiguration->runtime?->backend?->type);
    }
}
