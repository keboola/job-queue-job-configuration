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
            StateDefinition::NAMESPACE_COMPONENT => [],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertEquals($expected, $processed);
    }

    public function testComponentState(): void
    {
        $state = [
            StateDefinition::NAMESPACE_COMPONENT => ['key' => 'foo'],
        ];
        $expected = [
            StateDefinition::NAMESPACE_COMPONENT => ['key' => 'foo'],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertEquals($expected, $processed);
    }

    public function testStorageInputTablesState(): void
    {
        $state = [
            StateDefinition::NAMESPACE_STORAGE => [
                StateDefinition::NAMESPACE_INPUT => [
                    StateDefinition::NAMESPACE_TABLES => [
                        [
                            'source' => 'sourceTable',
                            'lastImportDate' => 'someDate',
                        ],
                    ],
                ],
            ],
        ];
        $expected = [
            StateDefinition::NAMESPACE_COMPONENT => [],
            StateDefinition::NAMESPACE_STORAGE => [
                StateDefinition::NAMESPACE_INPUT => [
                    StateDefinition::NAMESPACE_TABLES => [
                        [
                            'source' => 'sourceTable',
                            'lastImportDate' => 'someDate',
                        ],
                    ],
                    StateDefinition::NAMESPACE_FILES => [],
                ],
            ],
        ];
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertEquals($expected, $processed);
    }

    public function testStorageInputTablesStateExtraKey(): void
    {
        $state = [
            StateDefinition::NAMESPACE_STORAGE => [
                StateDefinition::NAMESPACE_INPUT => [
                    StateDefinition::NAMESPACE_TABLES => [
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
            StateDefinition::NAMESPACE_STORAGE => [
                StateDefinition::NAMESPACE_INPUT => [
                    StateDefinition::NAMESPACE_TABLES => [
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
            StateDefinition::NAMESPACE_STORAGE => [
                StateDefinition::NAMESPACE_INPUT => [
                    StateDefinition::NAMESPACE_FILES => [
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
            StateDefinition::NAMESPACE_COMPONENT => [],
            StateDefinition::NAMESPACE_STORAGE => [
                StateDefinition::NAMESPACE_INPUT => [
                    StateDefinition::NAMESPACE_TABLES => [],
                    StateDefinition::NAMESPACE_FILES => [
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
        $processed = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $state]);
        self::assertEquals($expected, $processed);
    }

    public function testStorageInputFilesStateExtraKey(): void
    {
        $state = [
            StateDefinition::NAMESPACE_STORAGE => [
                StateDefinition::NAMESPACE_INPUT => [
                    StateDefinition::NAMESPACE_FILES => [
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
            StateDefinition::NAMESPACE_STORAGE => [
                StateDefinition::NAMESPACE_INPUT => [
                    StateDefinition::NAMESPACE_FILES => [
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
}
