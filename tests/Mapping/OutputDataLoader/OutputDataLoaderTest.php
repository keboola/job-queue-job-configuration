<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use ColinODell\PsrTestLogger\TestLogger;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Output;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\StorageApiToken;
use Symfony\Component\Filesystem\Filesystem;
use Throwable;

class OutputDataLoaderTest extends BaseOutputDataLoaderTest
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanupBucketAndFiles();
    }

    public function testNoConfigDefaultBucketException(): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Configuration ID not set');
        $dataLoader = $this->getOutputDataLoader();
        $dataLoader->storeOutput(
            $this->getComponentWithDefaultBucket(),
            new Configuration(
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
        $storage = new Storage(
            input: new Input(
                tables: new TablesList([
                    'tables' => [
                        [
                            'source' => 'in.c-docker-demo-testConfig.test',
                        ],
                    ],
                ])
            ),
            output: new Output(
                tables: new TablesList([
                    [
                        'source' => 'sliced.csv',
                        'destination' => 'in.c-docker-demo-testConfig.out',
                        // erroneous lines
                        'primary_key' => 'col1',
                        'incremental' => 1,
                    ],
                ])
            ),
        );
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Failed to prepare mapping configuration for table sliced.csv: Invalid type for path "table.primary_key". Expected "array", but got "string"',
        );
        $dataLoader = $this->getOutputDataLoader();
        $dataLoader->storeOutput(
            $this->getComponent(),
            new Configuration(
                parameters: [],
                storage: $storage,
            ),
            null,
            null,
            'testConfig',
            null,
            projectFeatures: [],
        );
    }

    // todo: testWorkspaceInvalid - invalid staging provider, see below, should be validated in src/JobDefinition/Component/ComponentSpecificationDefinition.php:106 (?)
    // https://github.com/keboola/docker-bundle/blob/cd05830133f3588ff6bb671cc01ece1486e81dd3/src/Docker/Runner/DataLoader/DataLoader.php#L407

    public function testTypedTableCreate(): void
    {
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
                        'destination' => 'in.c-docker-demo-testConfig.fixed-type-test',
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
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN_FEATURE_NATIVE_TYPES'),
            ),
        );
        $dataLoader = $this->getOutputDataLoader($clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new Configuration(
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

        $tableDetails = $clientWrapper->getBasicClient()->getTable('in.c-docker-demo-testConfig.fixed-type-test');
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

    public function testTypedTableCreateWithAuthoritativeSchemaConfig(): void
    {
        $tableId = 'in.c-docker-demo-testConfig.authoritative-types-test';
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
                ])
            ),
        );
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN_FEATURE_NEW_NATIVE_TYPES'),
            ),
        );
        $dataLoader = $this->getOutputDataLoader($clientWrapper);

        // try {
        //     $clientWrapper->getBasicClient()->dropBucket('in.c-docker-demo-testConfig', ['force' => true]);
        // } catch (Throwable) {
        //     // ignore
        // } // todo
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new Configuration(
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

        $tableDetails = $clientWrapper->getBasicClient()->getTable($tableId);
        // todo: in previous test, it was 'in.c-docker-demo-testConfig.fixed-type-test' and I think it was a mistake
        // and this test rely only on running this test after one before
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

    public function testTypedTableCreateWithHintsSchemaConfig(): void
    {
        $tableId = 'in.c-hints-types.hints-types-test';
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
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN_FEATURE_NEW_NATIVE_TYPES'),
            ),
        );
        try {
            $clientWrapper->getBasicClient()->dropTable($tableId);
        } catch (Throwable) {
            // ignore
        }

        $dataLoader = $this->getOutputDataLoader($clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new Configuration(
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

        $tableDetails = $clientWrapper->getBasicClient()->getTable($tableId);
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

    public function testTypedTableCreateWithSchemaConfigMetadata(): void
    {
        $tableId = 'in.c-docker-demo-testConfigMetadata.fixed-type-test';
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
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN_FEATURE_NEW_NATIVE_TYPES'), // todo
            ),
        );
        $dataLoader = $this->getOutputDataLoader($clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new Configuration(
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

        $tableDetails = $clientWrapper->getBasicClient()->getTable($tableId);
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

    public function testTypedTableModifyTableStructure(): void
    {
        $tableId = 'in.c-testTypedTableModifyTableStructure.typed-test';
        $tableInfo = new MappingDestination($tableId);

        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN_FEATURE_NEW_NATIVE_TYPES'),
            ),
        );

        if ($clientWrapper->getBasicClient()->bucketExists($tableInfo->getBucketId())) {
            $clientWrapper->getBasicClient()->dropBucket(
                $tableInfo->getBucketId(),
                [
                    'force' => true,
                ],
            );
        }

        // prepare storage in project
        $clientWrapper->getBasicClient()->createBucket(
            $tableInfo->getBucketName(),
            $tableInfo->getBucketStage(),
        );
        $clientWrapper->getBasicClient()->createTableDefinition(
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
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN_FEATURE_NEW_NATIVE_TYPES'),
            ),
        );
        $dataLoader = $this->getOutputDataLoader($clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new Configuration(
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

        $tableDetails = $clientWrapper->getBasicClient()->getTable($tableId);
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

    public function testTypedTableLoadWithDatabaseColumnAliases(): void
    {
        $tableId = 'in.c-testTypedTableLoadWithDatabaseColumnAliases.typed-test';
        $tableInfo = new MappingDestination($tableId);

        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN_FEATURE_NEW_NATIVE_TYPES'),
            ),
        );

        if ($clientWrapper->getBasicClient()->bucketExists($tableInfo->getBucketId())) {
            $clientWrapper->getBasicClient()->dropBucket(
                $tableInfo->getBucketId(),
                [
                    'force' => true,
                ],
            );
        }

        // prepare storage in project
        $clientWrapper->getBasicClient()->createBucket(
            $tableInfo->getBucketName(),
            $tableInfo->getBucketStage(),
        );
        $clientWrapper->getBasicClient()->createTableDefinition(
            $tableInfo->getBucketId(),
            [
                'name' => $tableInfo->getTableName(),
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
                        'destination' => $tableId,
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
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN_FEATURE_NEW_NATIVE_TYPES'), // todo
            ),
        );
        $dataLoader = $this->getOutputDataLoader($clientWrapper);
        $tableQueue = $dataLoader->storeOutput(
            $component,
            new Configuration(
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

        $tableDetails = $clientWrapper->getBasicClient()->getTable($tableId);
        self::assertTrue($tableDetails['isTyped']);
    }

    /**
     * @dataProvider dataTypeSupportProvider
     */
    public function testDataTypeSupport(
        bool $hasFeature,
        ?string $componentType,
        ?string $configType,
        string $expectedType,
    ): void {
        $componentId = 'keboola.runner-workspace-test';
        $componentConfig = [
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
        ];
        if ($componentType) {
            $componentConfig['dataTypesConfiguration']['dataTypesSupport'] = $componentType;
        }
        $component = new ComponentSpecification($componentConfig);

        $clientMock = $this->createMock(BranchAwareClient::class);
        $clientMock->method('verifyToken')->willReturn($this->clientWrapper->getBasicClient()->verifyToken());

        $getTokenMock = $this->createMock(StorageApiToken::class);
        $getTokenMock->method('hasFeature')->with('new-native-types')->willReturn($hasFeature);

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBasicClient')->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')->willReturn($clientMock);
        $clientWrapperMock->method('getToken')->willReturn($getTokenMock);

        $configuration = new \Keboola\StorageApi\Options\Components\Configuration();
        $configuration->setName('testWorkspaceCleanup');
        $configuration->setComponentId($componentId);
        $configuration->setConfiguration([]);

        $componentsApi = new Components($this->clientWrapper->getBasicClient());
        $configId = $componentsApi->addConfiguration($configuration)['id'];

        $logger = new TestLogger(); //todo

        if ($configType) {
            $storage = new Storage(
                output: new Output(
                    dataTypeSupport: $configType,
                ),
            );
        }
        $dataLoader = $this->getOutputDataLoader($clientWrapperMock, $logger);
        $dataLoader->storeOutput(
            component: $component,
            jobConfiguration: new Configuration(
                storage: $storage ?? new Storage(),
            ),
            branchId: null,
            runId: null,
            configId: $configId,
            configRowId: null,
            projectFeatures: [],
        );

        //self::assertEquals($expectedType, $dataLoader->getDataTypeSupport());
    }

    public function dataTypeSupportProvider(): iterable
    {

        yield 'default-values' => [
            true,
            null,
            null,
            'none',
        ];

        yield 'component-override' => [
            true,
            'hints',
            null,
            'hints',
        ];

        yield 'config-override' => [
            true,
            null,
            'authoritative',
            'authoritative',
        ];

        yield 'component-config-override' => [
            true,
            'hints',
            'authoritative',
            'authoritative',
        ];

        yield 'component-override-without-feature' => [
            false,
            'hints',
            null,
            'none',
        ];

        yield 'config-override-without-feature' => [
            false,
            null,
            'authoritative',
            'none',
        ];

        yield 'component-config-override-without-feature' => [
            false,
            'hints',
            'authoritative',
            'none',
        ];
    }
}