<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Table\Result;
use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration as JobConfiguration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Keboola\StorageApi\Metadata;
use Symfony\Component\Filesystem\Filesystem;

class InputDataLoaderTest extends BaseInputDataLoaderTestCase
{
    protected const RESOURCE_SUFFIX = '-input';

    public function testBranchMappingDisabled(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');

        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $metadata->postBucketMetadata(
            $bucketId,
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
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
                input: new Input(
                    tables: new TablesList([
                        [
                            'source' => "$bucketId.test",
                            'destination' => 'test.csv',
                        ],
                    ]),
                ),
            ),
        );
        $dataLoader = $this->getInputDataLoader($component);
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            "The buckets \"$bucketId\" come from a development " .
            'branch and must not be used directly in input mapping.',
        );
        $dataLoader->loadInputData(
            component: $component,
            jobConfiguration: $jobConfiguration,
            jobState: new State(),
        );
    }

    public function testBranchMappingEnabled(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');

        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getTmpDirPath() . '/data.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $csv = new CsvFile($this->getTmpDirPath() . '/data.csv');
        $this->clientWrapper->getBasicClient()->createTable($bucketId, 'test', $csv);
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $metadata->postBucketMetadata(
            $bucketId,
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
        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
                input: new Input(
                    tables: new TablesList([
                        [
                            'source' => "$bucketId.test",
                            'destination' => 'test.csv',
                        ],
                    ]),
                ),
            ),
        );
        $dataLoader = $this->getInputDataLoader($component);
        $storageState = $dataLoader->loadInputData(
            component: $component,
            jobConfiguration: $jobConfiguration,
            jobState: new State(),
        );
        self::assertInstanceOf(Result::class, $storageState->inputTableResult);
        self::assertInstanceOf(InputFileStateList::class, $storageState->inputFileStateList);
    }
}
