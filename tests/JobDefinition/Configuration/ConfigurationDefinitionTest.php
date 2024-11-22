<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\ConfigurationDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\DataTypeSupport;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class ConfigurationDefinitionTest extends TestCase
{
    public function testConfiguration(): void
    {
        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'input' => [
                        'tables' => [
                            [
                                'source' => 'in.c-main.data',
                            ],
                        ],
                        'files' => [
                            [
                                'tags' => ['tag1', 'tag2'],
                                'query' => 'esquery',
                            ],
                        ],
                    ],
                    'output' => [
                        'default_bucket' => 'in.c-my-bucket',
                        'tables' => [
                            [
                                'source' => 'test.csv',
                                'destination' => 'out.c-main.data',
                            ],
                        ],
                        'files' => [
                            [
                                'source' => 'file',
                                'tags' => ['tag'],
                            ],
                        ],
                    ],
                ],
                'parameters' => [
                    ['var1' => 'val1'],
                    ['arr1' => ['var2' => 'val2']],
                ],
                'authorization' => [
                    'oauth_api' => [
                        'id' => 1234,
                        'credentials' => [
                            'token' => '123456',
                            'params' => [
                                'key' => 'val',
                            ],
                        ],
                    ],
                    'context' => 'wlm',
                ],
                'processors' => [
                    'before' => [
                        [
                            'definition' => [
                                'component' => 'a',
                            ],
                            'parameters' => [
                                'key' => 'val',
                            ],
                        ],
                    ],
                    'after' => [
                        [
                            'definition' => [
                                'component' => 'a',
                            ],
                            'parameters' => [
                                'key' => 'val',
                            ],
                        ],
                    ],
                ],
                'variables_id' => '12',
                'variables_values_id' => '21',
                'shared_code_id' => '34',
                'shared_code_row_ids' => ['345', '435'],
            ],
        ]);
        self::assertTrue(true);
    }

    public function testConfigurationWithWorkspaceConnection(): void
    {
        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'input' => [
                        'tables' => [
                            [
                                'source' => 'in.c-main.data',
                            ],
                        ],
                        'files' => [
                            [
                                'tags' => ['tag1', 'tag2'],
                                'query' => 'esquery',
                            ],
                        ],
                    ],
                    'output' => [
                        'tables' => [
                            [
                                'source' => 'test.csv',
                                'destination' => 'out.c-main.data',
                            ],
                        ],
                        'files' => [
                            [
                                'source' => 'file',
                                'tags' => ['tag'],
                            ],
                        ],
                    ],
                ],
                'parameters' => [
                    ['var1' => 'val1'],
                    ['arr1' => ['var2' => 'val2']],
                ],
                'authorization' => [
                    'workspace' => [
                        'container' => 'my-container',
                        'connectionString' => 'aVeryLongString',
                        'account' => 'test',
                        'region' => 'mordor',
                        'credentials' => [
                            'client_id' => 'client123',
                            'private_key' => 'very-secret-private-key',
                        ],
                    ],
                    'context' => 'wlm',
                ],
                'processors' => [
                    'before' => [
                        [
                            'definition' => [
                                'component' => 'a',
                            ],
                            'parameters' => [
                                'key' => 'val',
                            ],
                        ],
                    ],
                    'after' => [
                        [
                            'definition' => [
                                'component' => 'a',
                            ],
                            'parameters' => [
                                'key' => 'val',
                            ],
                        ],
                    ],
                ],
                'variables_id' => '12',
                'variables_values_id' => '21',
                'shared_code_id' => '34',
                'shared_code_row_ids' => ['345', '435'],
            ],
        ]);
        self::assertTrue(true);
    }

    public function testRuntimeConfiguration(): void
    {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'runtime' => [
                    'safe' => true,
                    'image_tag' => '12.7.0',
                    'backend' => [
                        'type' => 'foo',
                        'context' => 'wml',
                        'workspace_credentials' => [
                            'id' => '1234',
                            'type' => 'snowflake',
                            '#password' => 'test',
                        ],
                    ],
                ],
            ],
        ]);

        self::assertSame([
            'type' => 'foo',
            'context' => 'wml',
            'workspace_credentials' => [
                'id' => '1234',
                'type' => 'snowflake',
                '#password' => 'test',
            ],
        ], $config['runtime']['backend']);
    }

    public function testRuntimeBackendConfigurationHasDefaultEmptyValue(): void
    {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'runtime' => [],
            ],
        ]);

        self::assertSame([], $config['runtime']['backend']);
    }

    public function testRuntimeBackendConfigurationIgnoreExtraKeys(): void
    {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'foo',
                        'context' => 'wml',
                        'extraKey' => 'ignored',
                    ],
                ],
            ],
        ]);

        self::assertSame(
            [
                'type' => 'foo',
                'context' => 'wml',
            ],
            $config['runtime']['backend'],
        );
    }

    public function testConfigurationWithTableFiles(): void
    {
        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'input' => [
                        'tables' => [],
                        'files' => [],
                    ],
                    'output' => [
                        'tables' => [],
                        'files' => [],
                        'table_files' => [
                            'tags' => ['tag'],
                        ],
                    ],
                ],
            ],
        ]);
        self::assertTrue(true);
    }

    public function testArtifactsConfigurationDoesNotAcceptsExtraKeys(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('Unrecognized option "backend" under "configuration.artifacts".');

        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'artifacts' => [
                    'backend' => [
                        'type' => 'foo',
                    ],
                ],
            ],
        ]);
    }

    public static function artifactsRunsConfigurationData(): iterable
    {
        yield 'empty configuration' => [
            [],
            [
                'enabled' => false,
            ],
        ];
        yield 'enabled filter - limit' => [
            [
                'enabled' => true,
                'filter' => [
                    'limit' => 3,
                ],
            ],
            [
                'enabled' => true,
                'filter' => [
                    'limit' => 3,
                ],
            ],
        ];
        yield 'enabled filter - date_since' => [
            [
                'enabled' => true,
                'filter' => [
                    'date_since' => '-7 days',
                ],
            ],
            [
                'enabled' => true,
                'filter' => [
                    'date_since' => '-7 days',
                ],
            ],
        ];
        yield 'enabled filter - limit + date_since' => [
            [
                'enabled' => true,
                'filter' => [
                    'limit' => 1,
                    'date_since' => '-7 days',
                ],
            ],
            [
                'enabled' => true,
                'filter' => [
                    'limit' => 1,
                    'date_since' => '-7 days',
                ],
            ],
        ];
    }

    /**
     * @dataProvider artifactsRunsConfigurationData
     */
    public function testArtifactsRunsConfiguration(
        array $runsConfiguration,
        array $expectedRunsConfiguration,
    ): void {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'artifacts' => [
                    'runs' => $runsConfiguration,
                ],
            ],
        ]);

        self::assertSame($expectedRunsConfiguration, $config['artifacts']['runs']);
    }

    public static function artifactsRunsConfigurationThrowsErrorOnInvalidConfigData(): iterable
    {
        yield 'enabled - empty configuration' => [
            [
                'enabled' => true,
            ],
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'Invalid configuration for path "configuration.artifacts.runs": At least one of "date_since" or "limit" parameters must be defined.',
        ];
        yield 'enabled - invalid enabled value' => [
            [
                'enabled' => 'a',
            ],
            'Invalid type for path "configuration.artifacts.runs.enabled". Expected "bool", but got "string".',
        ];
        yield 'enabled - invalid limit value' => [
            [
                'enabled' => true,
                'filter' => [
                    'limit' => 'a',
                ],
            ],
            'Invalid type for path "configuration.artifacts.runs.filter.limit". Expected "int", but got "string".',
        ];
        yield 'enabled - invalid date_since value' => [
            [
                'enabled' => true,
                'filter' => [
                    'date_since' => [],
                ],
            ],
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'Invalid type for path "configuration.artifacts.runs.filter.date_since". Expected "scalar", but got "array".',
        ];
        yield 'extrakeys' => [
            [
                'foo' => 'bar',
            ],
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'Unrecognized option "foo" under "configuration.artifacts.runs". Available options are "enabled", "filter".',
        ];
    }

    /**
     * @dataProvider artifactsRunsConfigurationThrowsErrorOnInvalidConfigData
     */
    public function testArtifactsRunsConfigurationThrowsErrorOnInvalidConfig(
        array $runsConfiguration,
        string $expecterErrorMessage,
    ): void {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expecterErrorMessage);

        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'artifacts' => [
                    'runs' => $runsConfiguration,
                ],
            ],
        ]);
    }

    public static function artifactsSharedConfigurationData(): iterable
    {
        yield 'empty configuration' => [
            [],
            [
                'enabled' => false,
            ],
        ];
        yield 'enabled filter' => [
            [
                'enabled' => true,
            ],
            [
                'enabled' => true,
            ],
        ];
    }

    /**
     * @dataProvider artifactsSharedConfigurationData
     */
    public function testArtifactsSharedConfiguration(
        array $sharedConfiguration,
        array $expectedSharedConfiguration,
    ): void {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'artifacts' => [
                    'shared' => $sharedConfiguration,
                ],
            ],
        ]);

        self::assertSame($expectedSharedConfiguration, $config['artifacts']['shared']);
    }

    public static function artifactsSharedConfigurationThrowsErrorOnInvalidConfigData(): iterable
    {
        yield 'enabled - invalid enabled value' => [
            [
                'enabled' => 'a',
            ],
            'Invalid type for path "configuration.artifacts.shared.enabled". Expected "bool", but got "string".',
        ];
        yield 'extrakeys' => [
            [
                'foo' => 'bar',
            ],
            'Unrecognized option "foo" under "configuration.artifacts.shared". Available option is "enabled".',
        ];
    }

    /**
     * @dataProvider artifactsSharedConfigurationThrowsErrorOnInvalidConfigData
     */
    public function testArtifactsSharedConfigurationThrowsErrorOnInvalidConfig(
        array $sharedConfiguration,
        string $expecterErrorMessage,
    ): void {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expecterErrorMessage);

        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'artifacts' => [
                    'shared' => $sharedConfiguration,
                ],
            ],
        ]);
    }

    public static function artifactsCustomConfigurationData(): iterable
    {
        yield 'empty configuration' => [
            [],
            [
                'enabled' => false,
            ],
        ];
        yield 'enabled filter - component' => [
            [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.orchestrator',
                ],
            ],
            [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.orchestrator',
                ],
            ],
        ];
        yield 'enabled filter - config_id' => [
            [
                'enabled' => true,
                'filter' => [
                    'config_id' => '123456',
                ],
            ],
            [
                'enabled' => true,
                'filter' => [
                    'config_id' => '123456',
                ],
            ],
        ];
        yield 'enabled filter - branch_id' => [
            [
                'enabled' => true,
                'filter' => [
                    'branch_id' => 'main',
                ],
            ],
            [
                'enabled' => true,
                'filter' => [
                    'branch_id' => 'main',
                ],
            ],
        ];
        yield 'enabled filter' => [
            [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.orchestrator',
                    'config_id' => '123456',
                    'branch_id' => 'main',
                ],
            ],
            [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.orchestrator',
                    'config_id' => '123456',
                    'branch_id' => 'main',
                ],
            ],
        ];
        yield 'enabled filter - limit' => [
            [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.orchestrator',
                    'config_id' => '123456',
                    'branch_id' => 'main',
                    'limit' => 123,
                ],
            ],
            [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.orchestrator',
                    'config_id' => '123456',
                    'branch_id' => 'main',
                    'limit' => 123,
                ],
            ],
        ];
        yield 'enabled filter - date_since' => [
            [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.orchestrator',
                    'config_id' => '123456',
                    'branch_id' => 'main',
                    'date_since' => '-7 days',
                ],
            ],
            [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.orchestrator',
                    'config_id' => '123456',
                    'branch_id' => 'main',
                    'date_since' => '-7 days',
                ],
            ],
        ];
    }

    /**
     * @dataProvider artifactsCustomConfigurationData
     */
    public function testArtifactsCustomConfiguration(
        array $customConfiguration,
        array $expectedCustomConfiguration,
    ): void {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'artifacts' => [
                    'custom' => $customConfiguration,
                ],
            ],
        ]);

        self::assertSame($expectedCustomConfiguration, $config['artifacts']['custom']);
    }

    public static function artifactsCustomConfigurationThrowsErrorOnInvalidConfigData(): iterable
    {
        yield 'enabled - empty configuration' => [
            [
                'enabled' => true,
            ],
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'Invalid configuration for path "configuration.artifacts.custom": "component_id", "config_id" and "branch_id" parameters must be defined.',
        ];
        yield 'enabled - invalid enabled value' => [
            [
                'enabled' => 'a',
            ],
            'Invalid type for path "configuration.artifacts.custom.enabled". Expected "bool", but got "string".',
        ];
        yield 'enabled - invalid limit value' => [
            [
                'enabled' => true,
                'filter' => [
                    'limit' => 'a',
                ],
            ],
            'Invalid type for path "configuration.artifacts.custom.filter.limit". Expected "int", but got "string".',
        ];
        yield 'enabled - invalid date_since value' => [
            [
                'enabled' => true,
                'filter' => [
                    'date_since' => [],
                ],
            ],
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'Invalid type for path "configuration.artifacts.custom.filter.date_since". Expected "scalar", but got "array".',
        ];
        yield 'extrakeys' => [
            [
                'foo' => 'bar',
            ],
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
            'Unrecognized option "foo" under "configuration.artifacts.custom". Available options are "enabled", "filter".',
        ];
    }

    /**
     * @dataProvider artifactsCustomConfigurationThrowsErrorOnInvalidConfigData
     */
    public function testArtifactsCustomConfigurationThrowsErrorOnInvalidConfig(
        array $customConfiguration,
        string $expecterErrorMessage,
    ): void {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expecterErrorMessage);

        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'artifacts' => [
                    'custom' => $customConfiguration,
                ],
            ],
        ]);
    }

    public function testArtifactsHavingMultipleFiltersEnabled(): void
    {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'artifacts' => [
                    'runs' => [
                        'enabled' => true,
                        'filter' => [
                            'limit' => 1,
                        ],
                    ],
                    'custom' => [
                        'enabled' => true,
                        'filter' => [
                            'component_id' => 'keboola.orchestrator',
                        ],
                    ],
                    'shared' => [
                        'enabled' => true,
                    ],
                ],
            ],
        ]);

        self::assertSame([
            'runs' => [
                'enabled' => true,
                'filter' => [
                    'limit' => 1,
                ],
            ],
            'custom' => [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.orchestrator',
                ],
            ],
            'shared' => ['enabled' => true],
        ], $config['artifacts']);
    }

    public function testConfigurationWithReadonlyRole(): void
    {
        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'input' => [
                        'read_only_storage_access' => true,
                        'tables' => [],
                        'files' => [],
                    ],
                    'output' => [
                        'tables' => [],
                        'files' => [],
                        'table_files' => [
                            'tags' => ['tag'],
                        ],
                    ],
                ],
            ],
        ]);
        self::assertTrue(true);
    }

    public function testConfigurationWithDataTypes(): void
    {
        // default value
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'output' => [
                        'tables' => [],
                    ],
                ],
            ],
        ]);
        self::assertArrayNotHasKey('data_type_support', $config['storage']['output']);

        // custom value
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'output' => [
                        'data_type_support' => 'authoritative',
                        'tables' => [],
                    ],
                ],
            ],
        ]);
        self::assertEquals(
            DataTypeSupport::AUTHORITATIVE->value,
            $config['storage']['output']['data_type_support'],
        );
    }

    public function testConfigurationWithInvalidDataTypesValue(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The value "invalid" is not allowed for path "configuration.storage.output.data_type_support". ' .
            'Permissible values: "authoritative", "hints", "none"',
        );
        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'output' => [
                        'data_type_support' => 'invalid',
                        'tables' => [],
                    ],
                ],
            ],
        ]);
    }
}
