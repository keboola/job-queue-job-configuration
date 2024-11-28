<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration as JobConfiguration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\DataTypeSupport;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Output;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use Keboola\JobQueue\JobConfiguration\Mapping\OutputDataLoader;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\Attribute\UseSnowflakeProject;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;
use Psr\Log\NullLogger;
use ReflectionClass;
use Symfony\Component\Filesystem\Filesystem;

class OutputDataLoaderTest extends BaseOutputDataLoaderTestCase
{
    public function testExecutorDefaultBucket(): void
    {
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv.manifest',
            (string) json_encode(['destination' => 'sliced']),
        );
        $dataLoader = $this->getOutputDataLoader();
        $tableQueue = $dataLoader->storeOutput(
            $this->getComponentWithDefaultBucket(),
            new JobConfiguration(),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
        self::assertNotNull($tableQueue);

        $tableQueue->waitForAll();
        $bucketId = self::getBucketIdByDisplayName(
            clientWrapper: $this->clientWrapper,
            bucketDisplayName: $this->getResourceName(),
            stage: 'in',
        );
        self::assertTrue(
            $this->clientWrapper->getBasicClient()->tableExists($bucketId . '.sliced'),
        );
        self::assertEquals([], $dataLoader->getWorkspaceCredentials());
        self::assertNull($dataLoader->getWorkspaceBackendSize());
    }

    public function testExecutorDefaultBucketOverride(): void
    {
        $bucketId = self::getBucketIdByDisplayName($this->clientWrapper, 'test-override', 'in');
        if ($bucketId) {
            try {
                $this->clientWrapper->getBasicClient()->dropBucket(
                    $bucketId,
                    ['force' => true, 'async' => true],
                );
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
        }

        $bucketId = $this->clientWrapper->getBasicClient()->createBucket('test-override', 'in');

        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv.manifest',
            (string) json_encode(['destination' => 'sliced']),
        );
        $component = $this->getComponentWithDefaultBucket();
        $dataLoader = $this->getOutputDataLoader();
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                parameters: [],
                storage: new Storage(
                    output: new Output(
                        defaultBucket: $bucketId,
                    ),
                ),
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
        self::assertNotNull($tableQueue);

        $tableQueue->waitForAll();
        $legacyPrefixDisabled = $this->clientWrapper
            ->getToken()
            ->hasFeature('disable-legacy-bucket-prefix');
        self::assertFalse($this->clientWrapper->getBasicClient()->tableExists(
            $component->getDefaultBucketName('testConfig') . '.sliced',
        ),);
        self::assertTrue($this->clientWrapper->getBasicClient()->tableExists($bucketId . '.sliced'));
        self::assertEquals([], $dataLoader->getWorkspaceCredentials());
        self::assertNull($dataLoader->getWorkspaceBackendSize());

        $this->clientWrapper->getBasicClient()->dropBucket(
            $bucketId,
            ['force' => true, 'async' => true],
        );
    }

    public function testNoConfigDefaultBucketException(): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Configuration ID not set');
        $dataLoader = $this->getOutputDataLoader();
        $dataLoader->storeOutput(
            $this->getComponentWithDefaultBucket(),
            new JobConfiguration(
                parameters: [],
                storage: new Storage(),
            ),
            null,
            null,
            null,
            null,
            projectFeatures: [],
        );
    }

    public function testExecutorInvalidOutputMapping(): void
    {
        $this->markTestSkipped('Will be implemented in separate PR, see Jira issue PST-2213');
    }

    /** @dataProvider invalidStagingProvider */
    public function testWorkspaceInvalid(string $input, string $output, string $error): void
    {
        $component = new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => $input,
                    'output' => $output,
                ],
            ],
        ]);
        $this->expectException(ApplicationException::class);
        $this->expectExceptionMessage($error);
        $dataLoader = $this->getOutputDataLoader();
        $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                storage: new Storage(),
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
    }

    public function invalidStagingProvider(): array
    {
        return [
            'snowflake-redshift' => [
                'workspace-snowflake',
                'workspace-redshift',
                'Component staging setting mismatch - input: "workspace-snowflake", output: "workspace-redshift".',
            ],
            'redshift-snowflake' => [
                'workspace-redshift',
                'workspace-snowflake',
                'Component staging setting mismatch - input: "workspace-redshift", output: "workspace-snowflake".',
            ],
            'snowflake-synapse' => [
                'workspace-snowflake',
                'workspace-synapse',
                'Component staging setting mismatch - input: "workspace-snowflake", output: "workspace-synapse".',
            ],
            'redshift-synapse' => [
                'workspace-redshift',
                'workspace-synapse',
                'Component staging setting mismatch - input: "workspace-redshift", output: "workspace-synapse".',
            ],
            'synapse-snowflake' => [
                'workspace-synapse',
                'workspace-snowflake',
                'Component staging setting mismatch - input: "workspace-synapse", output: "workspace-snowflake".',
            ],
            'synapse-redshift' => [
                'workspace-synapse',
                'workspace-redshift',
                'Component staging setting mismatch - input: "workspace-synapse", output: "workspace-redshift".',
            ],
            'abs-snowflake' => [
                'workspace-abs',
                'workspace-snowflake',
                'Component staging setting mismatch - input: "workspace-abs", output: "workspace-snowflake".',
            ],
            'abs-redshift' => [
                'workspace-abs',
                'workspace-redshift',
                'Component staging setting mismatch - input: "workspace-abs", output: "workspace-redshift".',
            ],
            'abs-synapse' => [
                'workspace-abs',
                'workspace-synapse',
                'Component staging setting mismatch - input: "workspace-abs", output: "workspace-synapse".',
            ],
            'bigquery-snowflake' => [
                'workspace-bigquery',
                'workspace-snowflake',
                'Component staging setting mismatch - input: "workspace-bigquery", output: "workspace-snowflake".',
            ],
        ];
    }

    public function testWorkspace(): void
    {
        $component = new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'workspace-snowflake',
                    'output' => 'workspace-snowflake',
                ],
            ],
        ]);
        $dataLoader = $this->getOutputDataLoader(
            componentStagingStorageType: AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
        );
        $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                storage: new Storage(),
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
        $credentials = $dataLoader->getWorkspaceCredentials();
        self::assertEquals(
            ['host', 'warehouse', 'database', 'schema', 'user', 'password', 'account'],
            array_keys($credentials),
        );
        self::assertNotEmpty($credentials['user']);
        self::assertNotNull($dataLoader->getWorkspaceBackendSize());
    }

    /**
     * @dataProvider readonlyFlagProvider
     */
    public function testWorkspaceReadOnly(bool $readOnlyWorkspace): void
    {
        $component = new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'workspace-snowflake',
                    'output' => 'workspace-snowflake',
                ],
            ],
        ]);
        $storage = new Storage(
            input: new Input(
                readOnlyStorageAccess: $readOnlyWorkspace,
            ),
        );
        $dataLoader = $this->getOutputDataLoader(
            componentStagingStorageType: AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            readOnlyWorkspace: $readOnlyWorkspace,
        );
        $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                storage: $storage,
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
        $credentials = $dataLoader->getWorkspaceCredentials();

        $schemaName = $credentials['schema'];
        $workspacesApi = new Workspaces($this->clientWrapper->getBasicClient());
        $workspaces = $workspacesApi->listWorkspaces();
        $readonlyWorkspace = null;
        foreach ($workspaces as $workspace) {
            if ($workspace['connection']['schema'] === $schemaName) {
                $readonlyWorkspace = $workspace;
            }
        }
        self::assertNotNull($readonlyWorkspace);
        self::assertSame($readOnlyWorkspace, $readonlyWorkspace['readOnlyStorageAccess']);
        $this->getWorkspaceCleaner(
            clientWrapper: $this->clientWrapper,
            configId: null,
            component: $component,
        )->cleanWorkspace($component, null);
    }

    public function readonlyFlagProvider(): Generator
    {
        yield 'readonly on' => [true];
        yield 'readonly off' => [false];
    }

    #[UseSnowflakeProject(nativeTypes: 'native-types')]
    public function testTypedTableCreate(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/typed-data.csv',
            '1,text,123.45,3.3333,true,2020-02-02,2020-02-02 02:02:02',
        );
        $component = new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ]);
        $storage = new Storage(
            output: new Output(
                tables: new TablesList([
                    [
                        'source' => 'typed-data.csv',
                        'destination' => sprintf(
                            '%s.fixed-type-test',
                            $bucketId,
                        ),
                        'columns' => ['int', 'string', 'decimal', 'float', 'bool', 'date', 'timestamp'],
                        'primary_key' => ['int'],
                        'column_metadata' => [
                            'int' => (new GenericStorage('int', ['nullable' => false]))->toMetadata(),
                            'string' => (new GenericStorage(
                                'varchar',
                                ['length' => '17', 'nullable' => false],
                            ))->toMetadata(),
                            'decimal' => (new GenericStorage('decimal', ['length' => '10.2']))->toMetadata(),
                            'float' => (new GenericStorage('float'))->toMetadata(),
                            'bool' => (new GenericStorage('bool'))->toMetadata(),
                            'date' => (new GenericStorage('date'))->toMetadata(),
                            'timestamp' => (new GenericStorage('timestamp'))->toMetadata(),
                        ],
                    ],
                ]),
            ),
        );
        $dataLoader = $this->getOutputDataLoader($this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                parameters: [],
                storage: $storage,
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable(
            sprintf(
                '%s.fixed-type-test',
                self::getBucketIdByDisplayName($this->clientWrapper, $this->getResourceName(), 'in'),
            ),
        );
        self::assertTrue($tableDetails['isTyped']);

        $tableDefinitionColumns = $tableDetails['definition']['columns'];
        self::assertDataType($tableDefinitionColumns, 'int', Snowflake::TYPE_NUMBER);
        self::assertDataType($tableDefinitionColumns, 'string', Snowflake::TYPE_VARCHAR);
        self::assertDataType($tableDefinitionColumns, 'decimal', Snowflake::TYPE_NUMBER);
        self::assertDataType($tableDefinitionColumns, 'float', Snowflake::TYPE_FLOAT);
        self::assertDataType($tableDefinitionColumns, 'bool', Snowflake::TYPE_BOOLEAN);
        self::assertDataType($tableDefinitionColumns, 'date', Snowflake::TYPE_DATE);
        self::assertDataType($tableDefinitionColumns, 'timestamp', Snowflake::TYPE_TIMESTAMP_LTZ);
    }

    private static function assertDataType(array $columns, string $columnName, string $expectedType): void
    {
        $columnDefinition = current(array_filter($columns, fn(array $column) => $column['name'] === $columnName));
        self::assertSame($expectedType, $columnDefinition['definition']['type']);
    }

    #[UseSnowflakeProject(nativeTypes: 'new-native-types')]
    public function testTypedTableCreateWithAuthoritativeSchemaConfig(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');
        $tableId = $bucketId . '.authoritative-types-test';
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/typed-data.csv',
            '1,text,123.45,3.3333,true,2020-02-02,2020-02-02 02:02:02',
        );
        $component = new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
            'dataTypesConfiguration' => [
                'dataTypesSupport' => 'authoritative',
            ],
        ]);
        $storage = new Storage(
            output: new Output(
                tables: new TablesList([
                    [
                        'source' => 'typed-data.csv',
                        'destination' => $tableId,
                        'schema' => [
                            [
                                'name' => 'int',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::NUMERIC,
                                    ],
                                ],
                                'primary_key' => true,
                                'nullable' => false,
                            ],
                            [
                                'name' => 'string',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::STRING,
                                        'length' => '17',
                                    ],
                                ],
                                'nullable' => false,
                            ],
                            [
                                'name' => 'decimal',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::NUMERIC,
                                        'length' => '10,2',
                                    ],
                                ],
                            ],
                            [
                                'name' => 'float',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::FLOAT,
                                    ],
                                ],
                            ],
                            [
                                'name' => 'bool',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::BOOLEAN,
                                    ],
                                ],
                            ],
                            [
                                'name' => 'date',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::DATE,
                                    ],
                                ],
                            ],
                            [
                                'name' => 'timestamp',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::TIMESTAMP,
                                    ],
                                ],
                            ],
                        ],
                    ],
                ]),
            ),
        );
        $dataLoader = $this->getOutputDataLoader($this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                storage: $storage,
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertTrue($tableDetails['isTyped']);

        $tableDefinitionColumns = $tableDetails['definition']['columns'];

        self::assertEquals(['int'], $tableDetails['definition']['primaryKeysNames']);
        self::assertDataType($tableDefinitionColumns, 'int', Snowflake::TYPE_NUMBER);
        self::assertDataType($tableDefinitionColumns, 'string', Snowflake::TYPE_VARCHAR);
        self::assertDataType($tableDefinitionColumns, 'decimal', Snowflake::TYPE_NUMBER);
        self::assertDataType($tableDefinitionColumns, 'float', Snowflake::TYPE_FLOAT);
        self::assertDataType($tableDefinitionColumns, 'bool', Snowflake::TYPE_BOOLEAN);
        self::assertDataType($tableDefinitionColumns, 'date', Snowflake::TYPE_DATE);
        self::assertDataType($tableDefinitionColumns, 'timestamp', Snowflake::TYPE_TIMESTAMP_LTZ);
    }

    #[UseSnowflakeProject(nativeTypes: 'new-native-types')]
    public function testTypedTableCreateWithHintsSchemaConfig(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');
        $tableId = $bucketId . '.hints-types-test';
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/typed-data.csv',
            '1,text,123.45',
        );
        $component = new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
            'dataTypesConfiguration' => [
                'dataTypesSupport' => 'hints',
            ],
        ]);
        $storage = new Storage(
            output: new Output(
                tables: new TablesList([
                    [
                        'source' => 'typed-data.csv',
                        'destination' => $tableId,
                        'schema' => [
                            [
                                'name' => 'int',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::NUMERIC,
                                    ],
                                ],
                                'primary_key' => true,
                            ],
                            [
                                'name' => 'string',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::STRING,
                                        'length' => '17',
                                    ],
                                ],
                            ],
                            [
                                'name' => 'decimal',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::NUMERIC,
                                        'length' => '10,2',
                                    ],
                                ],
                            ],
                        ],
                    ],
                ]),
            ),
        );

        $dataLoader = $this->getOutputDataLoader($this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                storage: $storage,
            ),
            null,
            null,
            'testConfig',
            null,
            [],
        );
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertTrue($tableDetails['isTyped']);

        $tableDefinitionColumns = $tableDetails['definition']['columns'];

        self::assertDataType($tableDefinitionColumns, 'int', Snowflake::TYPE_VARCHAR);
        self::assertDataType($tableDefinitionColumns, 'string', Snowflake::TYPE_VARCHAR);
        self::assertDataType($tableDefinitionColumns, 'decimal', Snowflake::TYPE_VARCHAR);

        $columnMetadata = $tableDetails['columnMetadata'];
        self::assertArrayHasKey('int', $columnMetadata);

        $intColumnMetadata = array_values(array_filter(
            $tableDetails['columnMetadata']['int'],
            fn(array $metadata) =>
                in_array(
                    $metadata['key'],
                    ['KBC.datatype.basetype', 'KBC.datatype.length', 'KBC.datatype.nullable'],
                    true,
                ) && $metadata['provider'] === 'docker-demo',
        ));

        self::assertCount(2, $intColumnMetadata);
        self::assertEquals([
            ['key' => 'KBC.datatype.basetype', 'value' => 'NUMERIC', 'provider' => 'docker-demo'],
            ['key' => 'KBC.datatype.nullable', 'value' => '1', 'provider' => 'docker-demo'],
        ], array_map(function ($v) {
            unset($v['id'], $v['timestamp']);
            return $v;
        }, $intColumnMetadata));

        $stringColumnMetadata = array_values(array_filter(
            $tableDetails['columnMetadata']['string'],
            fn(array $metadata) =>
                in_array(
                    $metadata['key'],
                    ['KBC.datatype.basetype', 'KBC.datatype.length', 'KBC.datatype.nullable'],
                    true,
                ) && $metadata['provider'] === 'docker-demo',
        ));

        self::assertCount(3, $stringColumnMetadata);
        self::assertEquals([
            ['key' => 'KBC.datatype.basetype', 'value' => 'STRING', 'provider' => 'docker-demo'],
            ['key' => 'KBC.datatype.length', 'value' => '17', 'provider' => 'docker-demo'],
            ['key' => 'KBC.datatype.nullable', 'value' => '1', 'provider' => 'docker-demo'],
        ], array_map(function ($v) {
            unset($v['id'], $v['timestamp']);
            return $v;
        }, $stringColumnMetadata));

        $decimalColumnMetadata = array_values(array_filter(
            $tableDetails['columnMetadata']['decimal'],
            fn(array $metadata) =>
                in_array(
                    $metadata['key'],
                    ['KBC.datatype.basetype', 'KBC.datatype.length', 'KBC.datatype.nullable'],
                    true,
                ) && $metadata['provider'] === 'docker-demo',
        ));

        self::assertCount(3, $decimalColumnMetadata);
        self::assertEquals([
            ['key' => 'KBC.datatype.basetype', 'value' => 'NUMERIC', 'provider' => 'docker-demo'],
            ['key' => 'KBC.datatype.length', 'value' => '10,2', 'provider' => 'docker-demo'],
            ['key' => 'KBC.datatype.nullable', 'value' => '1', 'provider' => 'docker-demo'],
        ], array_map(function ($v) {
            unset($v['id'], $v['timestamp']);
            return $v;
        }, $decimalColumnMetadata));
    }

    #[UseSnowflakeProject(nativeTypes: 'new-native-types')]
    public function testTypedTableCreateWithSchemaConfigMetadata(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');
        $tableId = $bucketId . '.fixed-type-test';
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/typed-data.csv',
            '1,text',
        );
        $component = new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ]);
        $storage = new Storage(
            output: new Output(
                tables: new TablesList([
                    [
                        'source' => 'typed-data.csv',
                        'destination' => $tableId,
                        'description' => 'table description',
                        'table_metadata' => [
                            'key1' => 'value1',
                            'key2' => 'value2',
                        ],
                        'schema' => [
                            [
                                'name' => 'int',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::NUMERIC,
                                    ],
                                ],
                                'primary_key' => true,
                                'nullable' => false,
                                'metadata' => [
                                    'key1' => 'value1',
                                    'key2' => 'value2',
                                ],
                            ],
                            [
                                'name' => 'string',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::STRING,
                                        'length' => '17',
                                    ],
                                ],
                                'description' => 'column description',
                                'nullable' => false,
                            ],
                        ],
                    ],
                ]),
            ),
        );
        $dataLoader = $this->getOutputDataLoader($this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                storage: $storage,
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertTrue($tableDetails['isTyped']);

        $tableMetadata = array_values(array_filter(
            $tableDetails['metadata'],
            fn(array $metadata) => in_array($metadata['key'], ['key1', 'key2', 'KBC.description'], true),
        ));
        self::assertCount(3, $tableMetadata);
        self::assertEquals([
            ['key' => 'key1', 'value' => 'value1', 'provider' => 'docker-demo'],
            ['key' => 'key2', 'value' => 'value2', 'provider' => 'docker-demo'],
            ['key' => 'KBC.description', 'value' => 'table description', 'provider' => 'docker-demo'],
        ], array_map(function ($v) {
            unset($v['id'], $v['timestamp']);
            return $v;
        }, $tableMetadata));

        $intColumnMetadata = array_values(array_filter(
            $tableDetails['columnMetadata']['int'],
            fn(array $metadata) => in_array($metadata['key'], ['key1', 'key2'], true),
        ));
        self::assertCount(2, $intColumnMetadata);
        self::assertEquals([
            ['key' => 'key1', 'value' => 'value1', 'provider' => 'docker-demo'],
            ['key' => 'key2', 'value' => 'value2', 'provider' => 'docker-demo'],
        ], array_map(function ($v) {
            unset($v['id'], $v['timestamp']);
            return $v;
        }, $intColumnMetadata));

        $stringColumnMetadata = array_values(array_filter(
            $tableDetails['columnMetadata']['string'],
            fn(array $metadata) => in_array($metadata['key'], ['KBC.description'], true),
        ));
        self::assertCount(1, $stringColumnMetadata);
        self::assertEquals([
            ['key' => 'KBC.description', 'value' => 'column description', 'provider' => 'docker-demo'],
        ], array_map(function ($v) {
            unset($v['id'], $v['timestamp']);
            return $v;
        }, $stringColumnMetadata));
    }

    #[UseSnowflakeProject(nativeTypes: 'new-native-types')]
    public function testTypedTableModifyTableStructure(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');
        $tableId = $bucketId . '.typed-test';
        $tableInfo = new MappingDestination($tableId);

        $this->clientWrapper->getBasicClient()->createTableDefinition(
            $tableInfo->getBucketId(),
            [
                'name' => $tableInfo->getTableName(),
                'primaryKeysNames' => ['Id'],
                'columns' => [
                    [
                        'name' => 'Id',
                        'definition' => [
                            'type' => BaseType::STRING,
                            'nullable' => false,
                        ],
                    ],
                    [
                        'name' => 'Name',
                        'definition' => [
                            'type' => BaseType::STRING,
                            'length' => '255',
                            'nullable' => false,
                        ],
                    ],
                    [
                        'name' => 'foo',
                        'definition' => [
                            'type' => BaseType::STRING,
                            'length' => '255',
                            'nullable' => false,
                        ],
                    ],
                ],
            ],
        );

        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/typed-data.csv',
            '1,text,text2,text3',
        );

        $component = new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ]);
        $storage = new Storage(
            output: new Output(
                tables: new TablesList([
                    [
                        'source' => 'typed-data.csv',
                        'destination' => $tableId,
                        'description' => 'table description',
                        'table_metadata' => [
                            'key1' => 'value1',
                            'key2' => 'value2',
                        ],
                        'schema' => [
                            [
                                'name' => 'Id',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::STRING,
                                    ],
                                ],
                                'primary_key' => false,
                                'nullable' => false,
                            ],
                            [
                                'name' => 'Name',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::STRING,
                                        'length' => '255',
                                    ],
                                ],
                                'primary_key' => true,
                                'nullable' => false,
                            ],
                            [
                                'name' => 'foo',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::STRING,
                                        'length' => '500',
                                    ],
                                ],
                                'primary_key' => true,
                                'nullable' => false,
                            ],
                            [
                                'name' => 'New Column',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::STRING,
                                        'length' => '255',
                                    ],
                                ],
                                'nullable' => false,
                            ],
                        ],
                    ],
                ]),
            ),
        );
        $dataLoader = $this->getOutputDataLoader($this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                storage: $storage,
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertTrue($tableDetails['isTyped']);

        // PKs is changed
        self::assertEquals(['Name', 'foo'], $tableDetails['definition']['primaryKeysNames']);

        // length is changed
        self::assertEquals('500', $tableDetails['definition']['columns'][0]['definition']['length']);

        // nullable is changed
        self::assertFalse($tableDetails['definition']['columns'][1]['definition']['nullable']);

        // new column is added and Webalized
        self::assertEquals('New_Column', $tableDetails['definition']['columns'][3]['name']);
    }

    #[UseSnowflakeProject(nativeTypes: 'new-native-types')]
    public function testTypedTableLoadWithDatabaseColumnAliases(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');
        $this->clientWrapper->getBasicClient()->createTableDefinition(
            $bucketId,
            [
                'name' => 'typed-test',
                'primaryKeysNames' => [],
                'columns' => [
                    [
                        'name' => 'varchar',
                        'definition' => [
                            'type' => Snowflake::TYPE_VARCHAR,
                        ],
                    ],
                    [
                        'name' => 'number',
                        'definition' => [
                            'type' => Snowflake::TYPE_NUMBER,
                        ],
                    ],
                    [
                        'name' => 'float',
                        'definition' => [
                            'type' => Snowflake::TYPE_FLOAT,
                        ],
                    ],
                ],
            ],
        );

        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/typed-data.csv',
            '1,1,1.0',
        );

        $component = new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ]);
        $storage = new Storage(
            output: new Output(
                tables: new TablesList([
                    [
                        'source' => 'typed-data.csv',
                        'destination' => $bucketId . '.typed-test',
                        'description' => 'table description',
                        'table_metadata' => [
                            'key1' => 'value1',
                            'key2' => 'value2',
                        ],
                        'schema' => [
                            [
                                'name' => 'varchar',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::STRING,
                                    ],
                                    'snowflake' => [
                                        'type' => Snowflake::TYPE_NVARCHAR2,
                                    ],
                                ],
                            ],
                            [
                                'name' => 'number',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::INTEGER,
                                    ],
                                    'snowflake' => [
                                        'type' => Snowflake::TYPE_INTEGER,
                                    ],
                                ],
                            ],
                            [
                                'name' => 'float',
                                'data_type' => [
                                    'base' => [
                                        'type' => BaseType::FLOAT,
                                    ],
                                    'snowflake' => [
                                        'type' => Snowflake::TYPE_DOUBLE,
                                    ],
                                ],
                            ],
                        ],
                    ],
                ]),
            ),
        );
        $dataLoader = $this->getOutputDataLoader($this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                storage: $storage,
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($bucketId . '.typed-test');
        self::assertTrue($tableDetails['isTyped']);
    }

    public function testWorkspaceCleanupSuccess(): void
    {
        $componentId = 'keboola.runner-workspace-test';
        $component = new ComponentSpecification([
            'id' => $componentId,
            'data' => [
                'definition' => [
                    'type' => 'aws-ecr',
                    // phpcs:ignore Generic.Files.LineLength.MaxExceeded
                    'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-workspace-test',
                    'tag' => '1.6.2',
                ],
                'staging-storage' => [
                    'input' => 'workspace-snowflake',
                    'output' => 'workspace-snowflake',
                ],
            ],
        ]);
        $clientMock = $this->createMock(BranchAwareClient::class);
        $clientMock->method('verifyToken')->willReturn($this->clientWrapper->getBasicClient()->verifyToken());
        $configuration = new Configuration();
        $configuration->setName('testWorkspaceCleanup');
        $configuration->setComponentId($componentId);
        $configuration->setConfiguration([]);
        $componentsApi = new Components($this->clientWrapper->getBasicClient());
        $configId = $componentsApi->addConfiguration($configuration)['id'];

        $clientMock->expects(self::never())
            ->method('apiPostJson');
        $clientMock->expects(self::never())
            ->method('apiDelete');

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBasicClient')->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')->willReturn($clientMock);

        // immediately calling cleanWorkspace without using it means it was not initialized
        $this->getWorkspaceCleaner(
            clientWrapper: $clientWrapperMock,
            configId: $configId,
            component: $component,
        )->cleanWorkspace($component, $configId);

        $listOptions = new ListConfigurationWorkspacesOptions();
        $listOptions->setComponentId($componentId)->setConfigurationId($configId);
        $workspaces = $componentsApi->listConfigurationWorkspaces($listOptions);
        self::assertCount(0, $workspaces);
        $componentsApi->deleteConfiguration($componentId, $configId);
    }

    public function testWorkspaceCleanupWhenInitialized(): void
    {
        $this->markTestSkipped('Will be implemented in separate PR, see Jira issue PST-2213');
    }

    public function testWorkspaceCleanupFailure(): void
    {
        $this->markTestSkipped('Will be implemented in separate PR, see Jira issue PST-2213');
    }

    public function testExternallyManagedWorkspaceSuccess(): void
    {
        $this->markTestSkipped('Will be implemented in separate PR, see Jira issue PST-2213');
    }

    /**
     * @dataProvider dataTypeSupportProvider
     */
    public function testDataTypeSupport(
        bool $hasFeature,
        ?DataTypeSupport $componentType,
        ?DataTypeSupport $configType,
        DataTypeSupport $expectedType,
    ): void {
        $componentConfig = [
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'workspace-snowflake',
                    'output' => 'workspace-snowflake',
                ],
            ],
        ];

        if ($componentType !== null) {
            $componentConfig['dataTypesConfiguration']['dataTypesSupport'] = $componentType->value;
        }

        $component = new ComponentSpecification($componentConfig);

        $tokenMock = $this->createMock(StorageApiToken::class);
        $tokenMock->method('hasFeature')->with('new-native-types')->willReturn($hasFeature);

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getToken')->willReturn($tokenMock);

        $outputStrategyFactoryMock = $this->createMock(StrategyFactory::class);
        $outputStrategyFactoryMock->method('getClientWrapper')->willReturn($clientWrapperMock);

        $outputDataLoader = new OutputDataLoader($outputStrategyFactoryMock, new NullLogger, '/data/out');

        // Make getDataTypeSupport method accessible
        $reflector = new ReflectionClass(OutputDataLoader::class);
        $method = $reflector->getMethod('getDataTypeSupport');

        $outputStorageConfig = new Output(dataTypeSupport: $configType);

        $this->assertEquals($expectedType, $method->invoke($outputDataLoader, $component, $outputStorageConfig));
    }

    public function dataTypeSupportProvider(): iterable
    {

        yield 'default-values' => [
            true,
            null,
            null,
            DataTypeSupport::NONE,
        ];

        yield 'component-override' => [
            true,
            DataTypeSupport::HINTS,
            null,
            DataTypeSupport::HINTS,
        ];

        yield 'config-override' => [
            true,
            null,
            DataTypeSupport::AUTHORITATIVE,
            DataTypeSupport::AUTHORITATIVE,
        ];

        yield 'component-config-override' => [
            true,
            DataTypeSupport::HINTS,
            DataTypeSupport::AUTHORITATIVE,
            DataTypeSupport::AUTHORITATIVE,
        ];

        yield 'component-override-without-feature' => [
            false,
            DataTypeSupport::HINTS,
            null,
            DataTypeSupport::NONE,
        ];

        yield 'config-override-without-feature' => [
            false,
            null,
            DataTypeSupport::AUTHORITATIVE,
            DataTypeSupport::NONE,
        ];

        yield 'component-config-override-without-feature' => [
            false,
            DataTypeSupport::HINTS,
            DataTypeSupport::AUTHORITATIVE,
            DataTypeSupport::NONE,
        ];
    }
}
