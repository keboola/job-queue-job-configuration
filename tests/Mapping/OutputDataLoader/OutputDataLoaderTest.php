<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
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
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;
use Psr\Log\NullLogger;
use ReflectionClass;
use Symfony\Component\Filesystem\Filesystem;

class OutputDataLoaderTest extends BaseOutputDataLoaderTestCase
{
    protected static function expectedDefaultTableBackend(): string
    {
        return 'snowflake';
    }

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

        $component = $this->getComponentWithDefaultBucket();
        self::dropDefaultBucket(
            clientWrapper: $this->clientWrapper,
            component: $component,
            configId: 'testConfig',
        );

        $dataLoader = $this->getOutputDataLoader($component);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new JobConfiguration(),
            'testConfig',
            null,
        );
        self::assertNotNull($tableQueue);

        $tableQueue->waitForAll();
        $bucketId = self::getBucketIdByDisplayName(
            clientWrapper: $this->clientWrapper,
            bucketDisplayName: 'docker-demo-testConfig',
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
        $component = $this->getComponentWithDefaultBucket();
        self::dropDefaultBucket(
            clientWrapper: $this->clientWrapper,
            component: $component,
            configId: 'testConfig',
        );
        $bucketId = self::dropAndCreateBucket($this->clientWrapper, 'test-override', 'in');

        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv.manifest',
            (string) json_encode(['destination' => 'sliced']),
        );
        $dataLoader = $this->getOutputDataLoader($component);
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
            'testConfig',
            null,
        );
        self::assertNotNull($tableQueue);

        $tableQueue->waitForAll();
        self::assertFalse($this->clientWrapper->getBasicClient()->tableExists(
            $component->getDefaultBucketName('testConfig') . '.sliced',
        ));
        self::assertTrue($this->clientWrapper->getBasicClient()->tableExists($bucketId . '.sliced'));
        self::assertEquals([], $dataLoader->getWorkspaceCredentials());
        self::assertNull($dataLoader->getWorkspaceBackendSize());
    }

    public function testNoConfigDefaultBucketException(): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Configuration ID not set');
        $component = $this->getComponentWithDefaultBucket();
        self::dropDefaultBucket(
            clientWrapper: $this->clientWrapper,
            component: $component,
            configId: 'testConfig',
        );
        $dataLoader = $this->getOutputDataLoader($component);
        $dataLoader->storeOutput(
            $component,
            new JobConfiguration(
                parameters: [],
                storage: new Storage(),
            ),
            null,
            null,
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
        $dataLoader = $this->getOutputDataLoader($component);
        $dataLoader->storeOutput(
            $component,
            new JobConfiguration(),
            'testConfig',
            null,
        );
    }

    public static function invalidStagingProvider(): array
    {
        return [
            'snowflake-bigquery' => [
                'workspace-snowflake',
                'workspace-bigquery',
                'Component staging setting mismatch - input: "workspace-snowflake", output: "workspace-bigquery".',
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
        $dataLoader = $this->getOutputDataLoader($component);
        $dataLoader->storeOutput(
            $component,
            new JobConfiguration(),
            'testConfig',
            null,
        );
        $credentials = $dataLoader->getWorkspaceCredentials();
        self::assertEquals(
            ['host', 'warehouse', 'database', 'schema', 'user', 'password', 'privateKey', 'account'],
            array_keys($credentials),
        );
        self::assertNotEmpty($credentials['user']);
        self::assertNotNull($dataLoader->getWorkspaceCredentials());
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
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
                input: new Input(
                    readOnlyStorageAccess: $readOnlyWorkspace,
                ),
            ),
        );
        $dataLoader = $this->getOutputDataLoader(
            component: $component,
            readOnlyWorkspace: $readOnlyWorkspace,
        );
        $dataLoader->storeOutput(
            $component,
            $jobConfiguration,
            'testConfig',
            null,
        );
        $credentials = $dataLoader->getWorkspaceCredentials();

        $schemaName = $credentials['schema'];
        $workspacesApi = new Workspaces($this->clientWrapper->getBasicClient());
        $workspaces = $workspacesApi->listWorkspaces();
        $readonlyWorkspace = null;
        foreach ($workspaces as $workspace) {
            if (($workspace['connection']['schema'] ?? null) === $schemaName) {
                $readonlyWorkspace = $workspace;
            }
        }
        self::assertNotNull($readonlyWorkspace);
        self::assertSame($readOnlyWorkspace, $readonlyWorkspace['readOnlyStorageAccess']);
        $this->getWorkspaceCleaner(
            clientWrapper: $this->clientWrapper,
            configId: null,
            component: $component,
        )->cleanWorkspace();
    }

    public static function readonlyFlagProvider(): iterable
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
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
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
            ),
        );
        $dataLoader = $this->getOutputDataLoader($component, $this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            $jobConfiguration,
            'testConfig',
            null,
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
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
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
            ),
        );
        $dataLoader = $this->getOutputDataLoader($component, $this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            $jobConfiguration,
            'testConfig',
            null,
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
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
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
            ),
        );

        $dataLoader = $this->getOutputDataLoader($component, $this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            $jobConfiguration,
            'testConfig',
            null,
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
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
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
            ),
        );
        $dataLoader = $this->getOutputDataLoader($component, $this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            $jobConfiguration,
            'testConfig',
            null,
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
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
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
            ),
        );
        $dataLoader = $this->getOutputDataLoader($component, $this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            $jobConfiguration,
            'testConfig',
            null,
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
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
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
            ),
        );
        $dataLoader = $this->getOutputDataLoader($component, $this->clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            $jobConfiguration,
            'testConfig',
            null,
        );
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $tableDetails = $this->clientWrapper->getBasicClient()->getTable($bucketId . '.typed-test');
        self::assertTrue($tableDetails['isTyped']);
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

    public static function dataTypeSupportProvider(): iterable
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

    public function testTreatValuesAsNull(): void
    {
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/data.csv',
            '1,text,NAN',
        );
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/data.csv.manifest',
            (string) json_encode([
                'columns' => ['id', 'name', 'price'],
            ]),
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

        $config = JobConfiguration::fromArray([
            'storage' => [
                'output' => [
                    'treat_values_as_null' => ['NAN'],
                    'tables' => [
                        [
                            'source' => 'data.csv',
                            'destination' => 'in.c-docker-demo-testConfig.treated-values-test',
                        ],
                    ],
                ],
            ],
        ]);

        $this->dropAndCreateBucket($this->clientWrapper, 'docker-demo-testConfig', 'in');
        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition(
            'in.c-docker-demo-testConfig',
            [
                'name' => 'treated-values-test',
                'columns' => [
                    [
                        'name' => 'id',
                        'basetype' => BaseType::INTEGER,
                    ],
                    [
                        'name' => 'name',
                        'basetype' => BaseType::STRING,
                    ],
                    [
                        'name' => 'price',
                        'basetype' => BaseType::NUMERIC,
                    ],
                ],
            ],
        );

        $dataLoader = $this->getOutputDataLoader($component);

        $tableQueue = $dataLoader->storeOutput(
            component: $component,
            jobConfiguration: $config,
            configId: null,
            configRowId: null,
        );

        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        /** @var array|string $data */
        $data = $this->clientWrapper->getTableAndFileStorageClient()->getTableDataPreview(
            'in.c-docker-demo-testConfig.treated-values-test',
            [
                'format' => 'json',
            ],
        );

        self::assertIsArray($data);
        self::assertArrayHasKey('rows', $data);
        self::assertSame(
            [
                [
                    [
                        'columnName' => 'id',
                        'value' => '1',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'name',
                        'value' => 'text',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'price',
                        'value' => null,
                        'isTruncated' => false,
                    ],
                ],
            ],
            $data['rows'],
        );
    }

    public function testTreatValuesAsNullDisable(): void
    {
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/data.csv',
            '1,"",123',
        );
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/data.csv.manifest',
            (string) json_encode([
                'columns' => ['id', 'name', 'price'],
            ]),
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

        $config = JobConfiguration::fromArray([
            'storage' => [
                'output' => [
                    'treat_values_as_null' => [],
                    'tables' => [
                        [
                            'source' => 'data.csv',
                            'destination' => 'in.c-docker-demo-testConfig.treated-values-test',
                        ],
                    ],
                ],
            ],
        ]);

        $this->dropAndCreateBucket($this->clientWrapper, 'docker-demo-testConfig', 'in');
        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition(
            'in.c-docker-demo-testConfig',
            [
                'name' => 'treated-values-test',
                'columns' => [
                    [
                        'name' => 'id',
                        'basetype' => BaseType::INTEGER,
                    ],
                    [
                        'name' => 'name',
                        'basetype' => BaseType::STRING,
                    ],
                    [
                        'name' => 'price',
                        'basetype' => BaseType::INTEGER,
                    ],
                ],
            ],
        );

        $dataLoader = $this->getOutputDataLoader($component);

        $tableQueue = $dataLoader->storeOutput(
            component: $component,
            jobConfiguration: $config,
            configId: null,
            configRowId: null,
        );

        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        /** @var array|string $data */
        $data = $this->clientWrapper->getTableAndFileStorageClient()->getTableDataPreview(
            'in.c-docker-demo-testConfig.treated-values-test',
            [
                'format' => 'json',
            ],
        );

        self::assertIsArray($data);
        self::assertArrayHasKey('rows', $data);
        self::assertSame(
            [
                [
                    [
                        'columnName' => 'id',
                        'value' => '1',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'name',
                        'value' => '',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'price',
                        'value' => '123',
                        'isTruncated' => false,
                    ],
                ],
            ],
            $data['rows'],
        );
    }
}
