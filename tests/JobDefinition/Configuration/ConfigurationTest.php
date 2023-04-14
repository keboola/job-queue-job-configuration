<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Runtime;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use PHPUnit\Framework\TestCase;

class ConfigurationTest extends TestCase
{
    public function testConstructor(): void
    {
        $parameters = [
            'foo' => 'bar',
        ];

        $storage = Storage::fromArray([
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
        ]);

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

        $runtime = Runtime::fromArray([
            'backend' => [
                'type' => 'snowflake',
            ],
        ]);

        $configuration = new Configuration(
            parameters: $parameters,
            storage: $storage,
            processors: $processors,
            runtime: $runtime,
        );

        self::assertSame($parameters, $configuration->parameters);
        self::assertEquals($storage, $configuration->storage);
        self::assertSame($processors, $configuration->processors);
        self::assertEquals($runtime, $configuration->runtime);
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
        $parametersData = [
            'foo' => 'bar',
        ];

        $storageData = [
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

        $processorsData = [
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

        $runtimeData = [
            'backend' => [
                'type' => 'snowflake',
            ],
        ];

        $configuration = Configuration::fromArray([
            'parameters' => $parametersData,
            'storage' => $storageData,
            'processors' => $processorsData,
            'runtime' => $runtimeData,
        ]);

        self::assertSame($parametersData, $configuration->parameters);
        self::assertEquals(Storage::fromArray($storageData), $configuration->storage);
        self::assertSame($processorsData, $configuration->processors);
        self::assertEquals(Runtime::fromArray($runtimeData), $configuration->runtime);
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
