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


    public static function provideValidWorkspaceCredentials(): iterable
    {
        yield 'snowflake password' => [
            'data' => [
                'type' => 'snowflake',
                'id' => '1234',
                '#password' => 'test',
            ],
        ];

        yield 'snowflake privateKey' => [
            'data' => [
                'type' => 'snowflake',
                'id' => '1234',
                '#privateKey' => 'test',
            ],
        ];
    }

    /**
     * @dataProvider provideValidWorkspaceCredentials
     * @doesNotPerformAssertions
     */
    public function testValidWorkspaceConfiguration(array $data): void
    {
        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'config' => [
                'runtime' => [
                    'backend' => [
                        'workspace_credentials' => $data,
                    ],
                ],
            ],
        ]);
    }

    public static function provideInvalidWorkspaceCredentials(): iterable
    {
        yield 'missing id' => [
            'data' => [
                'type' => 'snowflake',
                '#password' => 'test',
            ],
            'expectedError' => 'The child config "id" under "configuration.runtime.backend.workspace_credentials" ' .
                'must be configured.',
        ];

        yield 'missing type' => [
            'data' => [
                'id' => '1234',
                '#password' => 'test',
            ],
            'expectedError' => 'The child config "type" under "configuration.runtime.backend.workspace_credentials" ' .
                'must be configured.',
        ];

        yield 'invalid type' => [
            'data' => [
                'id' => '1234',
                'type' => 'foo',
                '#password' => 'test',
            ],
            'expectedError' => 'The value "foo" is not allowed for path ' .
                '"configuration.runtime.backend.workspace_credentials.type". Permissible values: "snowflake"',
        ];

        yield 'no #password or #privateKey' => [
            'data' => [
                'id' => '1234',
                'type' => 'snowflake',
            ],
            'expectedError' => 'Invalid configuration for path "configuration.runtime.backend.workspace_credentials":' .
                ' Exactly one of "password" or "privateKey" must be configured.',
        ];

        yield 'both #password and #privateKey' => [
            'data' => [
                'id' => '1234',
                'type' => 'snowflake',
                '#password' => 'test',
                '#privateKey' => 'test',
            ],
            'expectedError' => 'Invalid configuration for path "configuration.runtime.backend.workspace_credentials":' .
                ' Exactly one of "password" or "privateKey" must be configured.',
        ];
    }

    /** @dataProvider provideInvalidWorkspaceCredentials */
    public function testInvalidWorkspaceConfiguration(array $data, string $expectedError): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedError);

        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'config' => [
                'runtime' => [
                    'backend' => [
                        'workspace_credentials' => $data,
                    ],
                ],
            ],
        ]);
    }

    public function testRuntimeBackendConfigurationHasDefaultEmptyValue(): void
    {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'runtime' => [],
            ],
        ]);

        self::assertSame([], $config['runtime']['backend']);

        self::assertArrayHasKey('process_timeout', $config['runtime']);
        self::assertNull($config['runtime']['process_timeout']);
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

    public function testRuntimeConfigurationInvalidWorkspaceCredentials(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        // phpcs:ignore Generic.Files.LineLength.MaxExceeded
        $this->expectExceptionMessage('The value "unsupported" is not allowed for path "configuration.runtime.backend.workspace_credentials.type". Permissible values: "snowflake"');

        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'config' => [
                'runtime' => [
                    'safe' => true,
                    'image_tag' => '12.7.0',
                    'backend' => [
                        'type' => 'foo',
                        'context' => 'wml',
                        'workspace_credentials' => [
                            'id' => '1234',
                            'type' => 'unsupported',
                            '#password' => 'test',
                        ],
                    ],
                ],
            ],
        ]);
    }

    public static function provideValidProcessTimeout(): iterable
    {
        yield 'value' => [
            'timeout' => 300,
        ];

        yield 'null' => [
            'timeout' => null,
        ];
    }

    /** @dataProvider provideValidProcessTimeout */
    public function testRuntimeProcessTimeoutSet(?int $timeout): void
    {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'config' => [
                'runtime' => [
                    'process_timeout' => $timeout,
                ],
            ],
        ]);

        self::assertArrayHasKey('process_timeout', $config['runtime']);
        self::assertSame($timeout, $config['runtime']['process_timeout']);
    }

    public static function provideInvalidProcessTimeout(): iterable
    {
        yield 'zero' => [
            'timeout' => 0,
            'expectedError' =>
                'Invalid configuration for path "configuration.runtime.process_timeout": must be greater than 0',
        ];

        yield 'negative' => [
            'timeout' => -10,
            'expectedError' =>
                'Invalid configuration for path "configuration.runtime.process_timeout": must be greater than 0',
        ];

        yield 'float' => [
            'timeout' => 10.0,
            'expectedError' =>
                'Invalid configuration for path "configuration.runtime.process_timeout": must be "null" or "int"',
        ];
    }

    /** @dataProvider provideInvalidProcessTimeout */
    public function testRuntimeProcessTimeoutInvalid(mixed $timeout, string $expectedError): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedError);

        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'config' => [
                'runtime' => [
                    'process_timeout' => $timeout,
                ],
            ],
        ]);
    }

    public function testRuntimeBackendConfigurationWithNullWorkspaceCredentials(): void
    {
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'runtime' => [
                    'backend' => [
                        'type' => 'foo',
                        'context' => 'wml',
                        'workspace_credentials' => null,
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

    public static function provideValidRuntimeProcessTimeoutConfigurationData(): iterable
    {
        yield 'valid integer value' => [
            'processTimeout' => 3600,
            'expectedValue' => 3600,
        ];

        yield 'null value (default)' => [
            'processTimeout' => null,
            'expectedValue' => null,
        ];
    }

    /** @dataProvider provideValidRuntimeProcessTimeoutConfigurationData */
    public function testValidRuntimeProcessTimeoutConfiguration(
        null|int $processTimeout,
        ?int $expectedValue,
    ): void {
        $config = [
            'configuration' => [
                'runtime' => [],
            ],
        ];

        if ($processTimeout !== null) {
            $config['configuration']['runtime']['process_timeout'] = $processTimeout;
        }

        $result = (new Processor())->processConfiguration(new ConfigurationDefinition(), $config);

        self::assertSame($expectedValue, $result['runtime']['process_timeout']);
    }

    public static function provideInvalidRuntimeProcessTimeoutConfigurationData(): iterable
    {
        yield 'non-integer value' => [
            'processTimeout' => 'invalid',
            'exceptionMessage' => 'must be "null" or "int"',
        ];

        yield 'float value' => [
            'processTimeout' => 3600.5,
            'exceptionMessage' => 'must be "null" or "int"',
        ];

        yield 'negative value' => [
            'processTimeout' => -1,
            'exceptionMessage' => 'must be greater than 0',
        ];

        yield 'zero value' => [
            'processTimeout' => 0,
            'exceptionMessage' => 'must be greater than 0',
        ];
    }

    /** @dataProvider provideInvalidRuntimeProcessTimeoutConfigurationData */
    public function testInvalidRuntimeProcessTimeoutConfiguration(
        mixed $processTimeout,
        string $exceptionMessage,
    ): void {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($exceptionMessage);

        $config = [
            'configuration' => [
                'runtime' => [
                    'process_timeout' => $processTimeout,
                ],
            ],
        ];

        (new Processor())->processConfiguration(new ConfigurationDefinition(), $config);
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

    public function testConfigurationWithTableModifications(): void
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
        self::assertArrayNotHasKey('table_modifications', $config['storage']['output']);

        // custom value
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'output' => [
                        'table_modifications' => 'non-destructive',
                        'tables' => [],
                    ],
                ],
            ],
        ]);
        self::assertSame(
            'non-destructive',
            $config['storage']['output']['table_modifications'],
        );
    }

    public function testConfigurationWithInvalidTableModificationsValue(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The value "invalid" is not allowed for path "configuration.storage.output.table_modifications". ' .
            'Permissible values: "none", "non-destructive", "all"',
        );
        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'output' => [
                        'table_modifications' => 'invalid',
                        'tables' => [],
                    ],
                ],
            ],
        ]);
    }

    public function testConfigurationWithTreatValuesAsNull(): void
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
        self::assertArrayNotHasKey('treat_values_as_null', $config['storage']['output']);

        // custom value - string
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'output' => [
                        'treat_values_as_null' => 'NULL',
                        'tables' => [],
                    ],
                ],
            ],
        ]);
        self::assertSame(
            'NULL',
            $config['storage']['output']['treat_values_as_null'],
        );

        // custom value - array
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'storage' => [
                    'output' => [
                        'treat_values_as_null' => ['NULL', 'N/A'],
                        'tables' => [],
                    ],
                ],
            ],
        ]);
        self::assertSame(
            ['NULL', 'N/A'],
            $config['storage']['output']['treat_values_as_null'],
        );
    }

    public function testConfigurationWithProcessorTag(): void
    {
        // Test with tag
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'processors' => [
                    'before' => [
                        [
                            'definition' => [
                                'component' => 'test-component',
                                'tag' => 'latest',
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        self::assertSame(
            'latest',
            $config['processors']['before'][0]['definition']['tag'],
        );

        // Test without tag (should be optional)
        $config = (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'processors' => [
                    'after' => [
                        [
                            'definition' => [
                                'component' => 'test-component',
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        self::assertArrayNotHasKey('tag', $config['processors']['after'][0]['definition']);
    }

    public static function provideInvalidProcessorDefinitionTestData(): iterable
    {
        foreach (['before', 'after'] as $type) {
            yield "$type, missing definition" => [
                'processorsData' => [
                    $type => [
                        [],
                    ],
                ],
                'expectedExceptionMessage' =>
                    "The child config \"definition\" under \"configuration.processors.$type.0\" must be configured.",
            ];
            yield "$type, empty definition" => [
                'processorsData' => [
                    $type => [
                        [
                            'definition' => [],
                        ],
                    ],
                ],
                'expectedExceptionMessage' =>
                    "The child config \"component\" under \"configuration.processors.$type.0.definition\""
                    . ' must be configured.',
            ];
            yield "$type, empty definition.component" => [
                'processorsData' => [
                    $type => [
                        [
                            'definition' => [
                                'component' => '',
                            ],
                        ],
                    ],
                ],
                'expectedExceptionMessage' =>
                    "The path \"configuration.processors.$type.0.definition.component\" cannot contain"
                    . ' an empty value, but got "".',
            ];
        }
    }

    /** @dataProvider provideInvalidProcessorDefinitionTestData */
    public function testConfigurationWithInvalidProcessorDefinition(
        array $processorsData,
        string $expectedExceptionMessage,
    ): void {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedExceptionMessage);

        (new Processor())->processConfiguration(new ConfigurationDefinition(), [
            'configuration' => [
                'processors' => $processorsData,
            ],
        ]);
    }
}
