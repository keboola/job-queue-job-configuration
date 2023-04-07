<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\StateSpec;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class StateSpecTest extends TestCase
{
    public function testEmptyState(): void
    {
        $state = [];
        $expected = [
            StateSpec::NAMESPACE_COMPONENT => [],
        ];
        $processed = (new Processor())->processConfiguration(new StateSpec(), ['state' => $state]);
        self::assertEquals($expected, $processed);
    }

    public function testComponentState(): void
    {
        $state = [
            StateSpec::NAMESPACE_COMPONENT => ['key' => 'foo'],
        ];
        $expected = [
            StateSpec::NAMESPACE_COMPONENT => ['key' => 'foo'],
        ];
        $processed = (new Processor())->processConfiguration(new StateSpec(), ['state' => $state]);
        self::assertEquals($expected, $processed);
    }

    public function testStorageInputTablesState(): void
    {
        $state = [
            StateSpec::NAMESPACE_STORAGE => [
                StateSpec::NAMESPACE_INPUT => [
                    StateSpec::NAMESPACE_TABLES => [
                        [
                            'source' => 'sourceTable',
                            'lastImportDate' => 'someDate',
                        ],
                    ],
                ],
            ],
        ];
        $expected = [
            StateSpec::NAMESPACE_COMPONENT => [],
            StateSpec::NAMESPACE_STORAGE => [
                StateSpec::NAMESPACE_INPUT => [
                    StateSpec::NAMESPACE_TABLES => [
                        [
                            'source' => 'sourceTable',
                            'lastImportDate' => 'someDate',
                        ],
                    ],
                    StateSpec::NAMESPACE_FILES => [],
                ],
            ],
        ];
        $processed = (new Processor())->processConfiguration(new StateSpec(), ['state' => $state]);
        self::assertEquals($expected, $processed);
    }

    public function testStorageInputTablesStateExtraKey(): void
    {
        $state = [
            StateSpec::NAMESPACE_STORAGE => [
                StateSpec::NAMESPACE_INPUT => [
                    StateSpec::NAMESPACE_TABLES => [
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
        (new Processor())->processConfiguration(new StateSpec(), ['state' => $state]);
    }

    public function testStorageInputTablesStateMissingKey(): void
    {
        $state = [
            StateSpec::NAMESPACE_STORAGE => [
                StateSpec::NAMESPACE_INPUT => [
                    StateSpec::NAMESPACE_TABLES => [
                        [
                            'source' => 'sourceTable',
                        ],
                    ],
                ],
            ],
        ];

        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage(
            'The child config "lastImportDate" under "state.storage.input.tables.0" must be configured'
        );
        (new Processor())->processConfiguration(new StateSpec(), ['state' => $state]);
    }

    public function testStorageInputFilesState(): void
    {
        $state = [
            StateSpec::NAMESPACE_STORAGE => [
                StateSpec::NAMESPACE_INPUT => [
                    StateSpec::NAMESPACE_FILES => [
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
            StateSpec::NAMESPACE_COMPONENT => [],
            StateSpec::NAMESPACE_STORAGE => [
                StateSpec::NAMESPACE_INPUT => [
                    StateSpec::NAMESPACE_TABLES => [],
                    StateSpec::NAMESPACE_FILES => [
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
        $processed = (new Processor())->processConfiguration(new StateSpec(), ['state' => $state]);
        self::assertEquals($expected, $processed);
    }

    public function testStorageInputFilesStateExtraKey(): void
    {
        $state = [
            StateSpec::NAMESPACE_STORAGE => [
                StateSpec::NAMESPACE_INPUT => [
                    StateSpec::NAMESPACE_FILES => [
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
        (new Processor())->processConfiguration(new StateSpec(), ['state' => $state]);
    }

    public function testStorageInputFilesStateMissingKey(): void
    {
        $state = [
            StateSpec::NAMESPACE_STORAGE => [
                StateSpec::NAMESPACE_INPUT => [
                    StateSpec::NAMESPACE_FILES => [
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
            'The child config "lastImportId" under "state.storage.input.files.0" must be configured'
        );
        (new Processor())->processConfiguration(new StateSpec(), ['state' => $state]);
    }

    public function testInvalidRootKey(): void
    {
        $state = [
            'invalidKey' => 'invalidValue',
        ];

        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('Unrecognized option "invalidKey" under "state"');
        (new Processor())->processConfiguration(new StateSpec(), ['state' => $state]);
    }
}
