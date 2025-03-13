<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\StateDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class StateDefinitionTest extends TestCase
{
    public function testEmptyState(): void
    {
        $state = [];
        $expected = [
            'component' => [],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertSame($expected, $processed);
    }

    public function testComponentState(): void
    {
        $state = [
            'component' => ['key' => 'foo'],
        ];
        $expected = [
            'component' => ['key' => 'foo'],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertSame($expected, $processed);
    }

    public function testComponentStateWithNonNormalizedKeys(): void
    {
        $state = [
            'component' => ['camelCase' => 'value', 'snake_case' => 'another_value'],
        ];
        $expected = [
            'component' => ['camelCase' => 'value', 'snake_case' => 'another_value'],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertSame($expected, $processed);
    }

    public function testStorageInputTablesState(): void
    {
        $state = [
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'sourceTable',
                            'lastImportDate' => 'someDate',
                        ],
                    ],
                ],
            ],
        ];
        $expected = [
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'sourceTable',
                            'lastImportDate' => 'someDate',
                        ],
                    ],
                    'files' => [],
                ],
            ],
            'component' => [],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertSame($expected, $processed);
    }

    public function testStorageInputTablesStateExtraKey(): void
    {
        $state = [
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'sourceTable',
                            'lastImportDate' => 'someDate',
                            'invalidKey' => 'invalidValue',
                        ],
                    ],
                ],
            ],
        ];

        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('Unrecognized option "invalidKey" under "state.storage.input.tables.0"');
        (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
    }

    public function testStorageInputTablesStateMissingKey(): void
    {
        $state = [
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'sourceTable',
                        ],
                    ],
                ],
            ],
        ];

        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage(
            'The child config "lastImportDate" under "state.storage.input.tables.0" must be configured',
        );
        (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
    }

    public function testStorageInputFilesState(): void
    {
        $state = [
            'storage' => [
                'input' => [
                    'files' => [
                        [
                            'tags' => [
                                [
                                    'name' => 'tag',
                                ],
                            ],
                            'lastImportId' => '12345',
                        ],
                    ],
                ],
            ],
        ];
        $expected = [
            'storage' => [
                'input' => [
                    'files' => [
                        [
                            'tags' => [
                                [
                                    'name' => 'tag',
                                ],
                            ],
                            'lastImportId' => '12345',
                        ],
                    ],
                    'tables' => [],
                ],
            ],
            'component' => [],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertSame($expected, $processed);
    }

    public function testStorageInputFilesStateExtraKey(): void
    {
        $state = [
            'storage' => [
                'input' => [
                    'files' => [
                        [
                            'tags' => [
                                [
                                    'name' => 'tag',
                                ],
                            ],
                            'lastImportId' => '12345',
                            'extraKey' => 'invalid',
                        ],
                    ],
                ],
            ],
        ];

        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('Unrecognized option "extraKey" under "state.storage.input.files.0"');
        (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
    }

    public function testStorageInputFilesStateMissingKey(): void
    {
        $state = [
            'storage' => [
                'input' => [
                    'files' => [
                        [
                            'tags' => [
                                [
                                    'name' => 'tag',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage(
            'The child config "lastImportId" under "state.storage.input.files.0" must be configured',
        );
        (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
    }

    public function testInvalidRootKey(): void
    {
        $state = [
            'invalidKey' => 'invalidValue',
        ];

        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('Unrecognized option "invalidKey" under "state"');
        (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
    }

    public function testDataAppState(): void
    {
        $state = [
            'data_app' => [
                'config' => 'value',
                'nested' => [
                    'property' => 123,
                ],
            ],
        ];
        $expected = [
            'data_app' => [
                'config' => 'value',
                'nested' => [
                    'property' => 123,
                ],
            ],
            'component' => [],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertSame($expected, $processed);
    }

    public function testDataAppStateWithNonNormalizedKeys(): void
    {
        $state = [
            'data_app' => [
                'camelCase' => 'value',
                'snake_case' => 'another_value',
                'nested' => [
                    'mixedCase_property' => 123,
                ],
            ],
        ];
        $expected = [
            'data_app' => [
                'camelCase' => 'value',
                'snake_case' => 'another_value',
                'nested' => [
                    'mixedCase_property' => 123,
                ],
            ],
            'component' => [],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertSame($expected, $processed);
    }

    public function testCompleteState(): void
    {
        $state = [
            'component' => ['key' => 'foo'],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'sourceTable',
                            'lastImportDate' => 'someDate',
                        ],
                    ],
                    'files' => [
                        [
                            'tags' => [
                                [
                                    'name' => 'tag',
                                ],
                            ],
                            'lastImportId' => '12345',
                        ],
                    ],
                ],
            ],
            'data_app' => [
                'config' => 'value',
            ],
        ];
        $expected = [
            'component' => ['key' => 'foo'],
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'sourceTable',
                            'lastImportDate' => 'someDate',
                        ],
                    ],
                    'files' => [
                        [
                            'tags' => [
                                [
                                    'name' => 'tag',
                                ],
                            ],
                            'lastImportId' => '12345',
                        ],
                    ],
                ],
            ],
            'data_app' => [
                'config' => 'value',
            ],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertSame($expected, $processed);
    }
}
