<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration as JobConfiguration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Output;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\Attribute\UseSnowflakeProject;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Symfony\Component\Filesystem\Filesystem;

class OutputDataLoaderMetadataTest extends BaseOutputDataLoaderTestCase
{
    private Metadata $metadata;

    protected function setUp(): void
    {
        parent::setUp();
        $this->metadata = new Metadata($this->clientWrapper->getBasicClient());
    }

    protected static function expectedDefaultTableBackend(): string
    {
        return 'snowflake';
    }

    /**
     * Transform metadata into a key-value array
     */
    private function getMetadataValues(array $metadata): array
    {
        $result = [];
        foreach ($metadata as $item) {
            $result[$item['provider']][$item['key']] = $item['value'];
        }
        return $result;
    }

    public function testDefaultSystemMetadata(): void
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

        $dataLoader = $this->getOutputDataLoader(
            config: new JobConfiguration(),
            component: $component,
        );
        $tableQueue = $dataLoader->storeOutput();
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $bucketId = self::getBucketIdByDisplayName($this->clientWrapper, 'docker-demo-testConfig', 'in');
        self::assertNotNull($bucketId);

        $bucketMetadata = $this->metadata->listBucketMetadata($bucketId);
        $expectedBucketMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'docker-demo',
                'KBC.createdBy.configuration.id' => 'testConfig',
            ],
        ];
        self::assertEquals($expectedBucketMetadata, $this->getMetadataValues($bucketMetadata));

        $tableMetadata = $this->metadata->listTableMetadata($bucketId . '.sliced');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'docker-demo',
                'KBC.createdBy.configuration.id' => 'testConfig',
                'KBC.lastUpdatedBy.component.id' => 'docker-demo',
                'KBC.lastUpdatedBy.configuration.id' => 'testConfig',
            ],
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        // let's run the data loader again.
        // This time the tables should receive 'update' metadata
        $tableQueue = $dataLoader->storeOutput();

        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();
        $tableMetadata = $this->metadata->listTableMetadata($bucketId . '.sliced');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'docker-demo';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'testConfig';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    public function createBranch(Client $client, string $branchName): int
    {
        $branches = new DevBranches($client);
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === $branchName) {
                $branches->deleteBranch($branch['id']);
            }
        }
        return $branches->createBranch($branchName)['id'];
    }

    #[UseSnowflakeProject(useMasterToken: true)]
    public function testDefaultSystemMetadataBranch(): void
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

        $branchId = $this->createBranch($this->clientWrapper->getBasicClient(), 'test-branch');
        $branchBucketName = sprintf('%s-%s', $branchId, 'docker-demo-testConfig');
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN_MASTER'),
                (string) $branchId,
            ),
        );

        $component = $this->getComponentWithDefaultBucket();
        $bucketId = self::getBucketIdByDisplayName($clientWrapper, $branchBucketName, 'in');
        if ($bucketId !== null) {
            $clientWrapper->getBasicClient()->dropBucket($bucketId);
        }

        $dataLoader = $this->getOutputDataLoader(
            config: new JobConfiguration(),
            component: $component,
            clientWrapper: $clientWrapper,
        );
        $tableQueue = $dataLoader->storeOutput();
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $bucketId = self::getBucketIdByDisplayName($clientWrapper, $branchBucketName, 'in');
        self::assertNotNull($bucketId);
        $bucketMetadata = $this->metadata->listBucketMetadata($bucketId);
        $expectedBucketMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'docker-demo',
                'KBC.createdBy.configuration.id' => 'testConfig',
                'KBC.createdBy.branch.id' => (string) $branchId,
            ],
        ];
        self::assertEquals($expectedBucketMetadata, $this->getMetadataValues($bucketMetadata));

        $tableMetadata = $this->metadata->listTableMetadata(sprintf(
            '%s.sliced',
            self::getBucketIdByDisplayName($clientWrapper, $branchBucketName, 'in'),
        ));
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'docker-demo',
                'KBC.createdBy.configuration.id' => 'testConfig',
                'KBC.createdBy.branch.id' => (string) $branchId,
                'KBC.lastUpdatedBy.component.id' => 'docker-demo',
                'KBC.lastUpdatedBy.configuration.id' => 'testConfig',
                'KBC.lastUpdatedBy.branch.id' => (string) $branchId,
            ],
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    public function testDefaultSystemConfigRowMetadata(): void
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

        $dataLoader = $this->getOutputDataLoader(
            config: new JobConfiguration(),
            component: $component,
            configRowId: 'testRow',
        );
        $tableQueue = $dataLoader->storeOutput();
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $bucketId = self::getBucketIdByDisplayName($this->clientWrapper, 'docker-demo-testConfig', 'in');
        self::assertNotNull($bucketId);
        $bucketMetadata = $this->metadata->listBucketMetadata($bucketId);
        $expectedBucketMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'docker-demo',
                'KBC.createdBy.configuration.id' => 'testConfig',
                'KBC.createdBy.configurationRow.id' => 'testRow',
            ],
        ];
        self::assertEquals($expectedBucketMetadata, $this->getMetadataValues($bucketMetadata));

        $tableMetadata = $this->metadata->listTableMetadata($bucketId . '.sliced');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'docker-demo',
                'KBC.createdBy.configuration.id' => 'testConfig',
                'KBC.createdBy.configurationRow.id' => 'testRow',
                'KBC.lastUpdatedBy.component.id' => 'docker-demo',
                'KBC.lastUpdatedBy.configuration.id' => 'testConfig',
                'KBC.lastUpdatedBy.configurationRow.id' => 'testRow',
            ],
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        // let's run the data loader again.
        // This time the tables should receive 'update' metadata
        $tableQueue = $dataLoader->storeOutput();
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $tableMetadata = $this->metadata->listTableMetadata($bucketId . '.sliced');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'docker-demo';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'testConfig';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configurationRow.id'] = 'testRow';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    public function testExecutorConfigMetadata(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');

        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );

        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
                input: new Input(
                    tables: new TablesList([
                        [
                            'source' => $bucketId . '.test',
                        ],
                    ]),
                ),
                output: new Output(
                    tables: new TablesList([
                        [
                            'source' => 'sliced.csv',
                            'destination' => $bucketId . '.out',
                            'metadata' => [
                                [
                                    'key' => 'table.key.one',
                                    'value' => 'table value one',
                                ],
                                [
                                    'key' => 'table.key.two',
                                    'value' => 'table value two',
                                ],
                            ],
                            'column_metadata' => [
                                'id' => [
                                    [
                                        'key' => 'column.key.one',
                                        'value' => 'column value one id',
                                    ],
                                    [
                                        'key' => 'column.key.two',
                                        'value' => 'column value two id',
                                    ],
                                ],
                                'text' => [
                                    [
                                        'key' => 'column.key.one',
                                        'value' => 'column value one text',
                                    ],
                                    [
                                        'key' => 'column.key.two',
                                        'value' => 'column value two text',
                                    ],
                                ],
                            ],
                        ],
                    ]),
                ),
            ),
        );
        $component = $this->getComponentWithDefaultBucket();
        self::dropDefaultBucket(
            clientWrapper: $this->clientWrapper,
            component: $component,
            configId: 'testConfig',
        );
        $dataLoader = $this->getOutputDataLoader(
            config: $jobConfiguration,
            component: $component,
        );
        $tableQueue = $dataLoader->storeOutput();
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();
        $tableMetadata = $this->metadata->listTableMetadata($bucketId . '.out');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.configuration.id' => 'testConfig',
                'KBC.createdBy.component.id' => 'docker-demo',
                'KBC.lastUpdatedBy.configuration.id' => 'testConfig',
                'KBC.lastUpdatedBy.component.id' => 'docker-demo',
            ],
            'docker-demo' => [
                'table.key.one' => 'table value one',
                'table.key.two' => 'table value two',
            ],
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        $idColMetadata = $this->metadata->listColumnMetadata($bucketId . '.out.id');
        $expectedColumnMetadata = [
            'docker-demo' => [
                'column.key.one' => 'column value one id',
                'column.key.two' => 'column value two id',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
    }

    public function testExecutorManifestMetadata(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');

        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $manifest = <<<JSON
        {
            "destination": "$bucketId.sliced",
            "metadata": [{
                    "key": "table.key.one",
                    "value": "table value one"
                },
                {
                    "key": "table.key.two",
                    "value": "table value two"
                }
            ],
            "column_metadata": {
                "id": [{
                        "key": "column.key.one",
                        "value": "column value one id"
                    },
                    {
                        "key": "column.key.two",
                        "value": "column value two id"
                    }
                ],
                "text": [{
                        "key": "column.key.one",
                        "value": "column value one text"
                    },
                    {
                        "key": "column.key.two",
                        "value": "column value two text"
                    }
                ]
            }
        }
        JSON;
        $fs->dumpFile($this->getDataDirPath() . '/out/tables/sliced.csv.manifest', $manifest);
        $component = $this->getComponentWithDefaultBucket();
        self::dropDefaultBucket(
            clientWrapper: $this->clientWrapper,
            component: $component,
            configId: 'testConfig',
        );
        $dataLoader = $this->getOutputDataLoader(
            config: new JobConfiguration(),
            component: $component,
        );
        $tableQueue = $dataLoader->storeOutput();
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();

        $tableMetadata = $this->metadata->listTableMetadata($bucketId . '.sliced');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.configuration.id' => 'testConfig',
                'KBC.createdBy.component.id' => 'docker-demo',
                'KBC.lastUpdatedBy.configuration.id' => 'testConfig',
                'KBC.lastUpdatedBy.component.id' => 'docker-demo',
            ],
            'docker-demo' => [
                'table.key.one' => 'table value one',
                'table.key.two' => 'table value two',
            ],
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        $idColMetadata = $this->metadata->listColumnMetadata($bucketId . '.sliced.id');
        $expectedColumnMetadata = [
            'docker-demo' => [
                'column.key.one' => 'column value one id',
                'column.key.two' => 'column value two id',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
    }

    public function testExecutorManifestMetadataCombined(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');

        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $fs->dumpFile(
            $this->getDataDirPath() . '/out/tables/sliced.csv.manifest',
            '{"metadata":[{"key":"table.key.one","value":"table value one"},'.
            '{"key":"table.key.two","value":"table value two"}],"column_metadata":{"id":['.
            '{"key":"column.key.one","value":"column value one id"},'.
            '{"key":"column.key.two","value":"column value two id"}],'.
            '"text":[{"key":"column.key.one","value":"column value one text"},'.
            '{"key":"column.key.two","value":"column value two text"}]}}',
        );
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
                input: new Input(
                    tables: new TablesList([
                        [
                            'source' => 'in.c-runner-test.test',
                        ],
                    ]),
                ),
                output: new Output(
                    tables: new TablesList([
                        [
                            'source' => 'sliced.csv',
                            'destination' => $bucketId . '.sliced',
                            'metadata' => [
                                [
                                    'key' => 'table.key.one',
                                    'value' => 'table value three',
                                ],
                                [
                                    'key' => 'table.key.two',
                                    'value' => 'table value four',
                                ],
                            ],
                            'column_metadata' => [
                                'id' => [
                                    [
                                        'key' => 'column.key.two',
                                        'value' => 'a new column value two id',
                                    ],
                                ],
                            ],
                        ],
                    ]),
                ),
            ),
        );

        $component = $this->getComponentWithDefaultBucket();
        self::dropDefaultBucket(
            clientWrapper: $this->clientWrapper,
            component: $component,
            configId: 'testConfig',
        );

        $dataLoader = $this->getOutputDataLoader(
            config: $jobConfiguration,
            component: $component,
        );
        $tableQueue = $dataLoader->storeOutput();
        self::assertNotNull($tableQueue);
        $tableQueue->waitForAll();
        $tableMetadata = $this->metadata->listTableMetadata($bucketId . '.sliced');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.configuration.id' => 'testConfig',
                'KBC.createdBy.component.id' => 'docker-demo',
                'KBC.lastUpdatedBy.configuration.id' => 'testConfig',
                'KBC.lastUpdatedBy.component.id' => 'docker-demo',
            ],
            'docker-demo' => [
                'table.key.one' => 'table value three',
                'table.key.two' => 'table value four',
            ],
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        $idColMetadata = $this->metadata->listColumnMetadata($bucketId . '.sliced.id');
        $expectedColumnMetadata = [
            'docker-demo' => [
                'column.key.one' => 'column value one id',
                'column.key.two' => 'a new column value two id',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
    }
}
