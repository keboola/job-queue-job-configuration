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
    }

    public function testFromEmptyArray(): void
    {
        $configuration = Configuration::fromArray([]);

        self::assertSame([], $configuration->parameters);
        self::assertEquals(new Storage(), $configuration->storage);
        self::assertSame([], $configuration->processors);
        self::assertNull($configuration->runtime);
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
        ]);

        self::assertSame($parameters, $configuration->parameters);
        self::assertEquals(Storage::fromArray($storage), $configuration->storage);
        self::assertSame($processors, $configuration->processors);
        self::assertEquals(Runtime::fromArray($runtime), $configuration->runtime);
    }

    public function testFromArrayWithInvalidData(): void
    {
        $this->expectException(ApplicationExceptionInterface::class);
        $this->expectExceptionMessage(
            'Job configuration is not valid: Unrecognized option "foo" under "configuration". Available options are'
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
                    'container_type' => null,
                    'context' => null,
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
