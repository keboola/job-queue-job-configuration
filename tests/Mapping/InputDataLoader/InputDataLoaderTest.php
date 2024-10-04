<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Table\Result;
use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Keboola\StorageApi\Metadata;
use Symfony\Component\Filesystem\Filesystem;

class InputDataLoaderTest extends BaseInputDataLoaderTest
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanupBucketAndFiles();
    }

    public function testBranchMappingDisabled(): void
    {
        $this->clientWrapper->getBasicClient()->createBucket('docker-demo-testConfig', 'in');
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $metadata->postBucketMetadata(
            'in.c-docker-demo-testConfig',
            'system',
            [
                [
                    'key' => 'KBC.createdBy.branch.id',
                    'value' => '1234',
                ],
            ],
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
            input: new Input(
                tables: new TablesList([
                    [
                        'source' => 'in.c-docker-demo-testConfig.test',
                        'destination' => 'test.csv',
                    ],
                ]),
            ),
        );
        $dataLoader = $this->getInputDataLoader();
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'The buckets "in.c-docker-demo-testConfig" come from a development ' .
            'branch and must not be used directly in input mapping.',
        );
        $dataLoader->loadInputData(
            component: $component,
            jobConfiguration: new Configuration(
                storage: $storage,
            ),
            jobState: new State(),
        );
    }

    public function testBranchMappingEnabled(): void
    {
        $this->clientWrapper->getBasicClient()->createBucket('docker-demo-testConfig', 'in');
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getTmpDirPath() . '/data.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $csv = new CsvFile($this->getTmpDirPath() . '/data.csv');
        $this->clientWrapper->getBasicClient()->createTable('in.c-docker-demo-testConfig', 'test', $csv);
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $metadata->postBucketMetadata(
            'in.c-docker-demo-testConfig',
            'system',
            [
                [
                    'key' => 'KBC.createdBy.branch.id',
                    'value' => '1234',
                ],
            ],
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
            'features' => ['dev-mapping-allowed'],
        ]);
        $storage = new Storage(
            input: new Input(
                tables: new TablesList([
                    [
                        'source' => 'in.c-docker-demo-testConfig.test',
                        'destination' => 'test.csv',
                    ],
                ]),
            ),
        );
        $dataLoader = $this->getInputDataLoader();
        $storageState = $dataLoader->loadInputData(
            component: $component,
            jobConfiguration: new Configuration(
                storage: $storage,
            ),
            jobState: new State(),
        );
        self::assertInstanceOf(Result::class, $storageState->inputTableResult);
        self::assertInstanceOf(InputFileStateList::class, $storageState->inputFileStateList);
    }
}
