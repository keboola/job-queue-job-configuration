<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration as JobConfiguration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Symfony\Component\Filesystem\Filesystem;

class BigQueryInputDataLoaderTest extends BaseInputDataLoaderTestCase
{
    protected const COMPONENT_ID = 'keboola.runner-workspace-bigquery-test';
    protected const DEFAULT_PROJECT = 'gcp';

    public function testWorkspaceBigQueryNoPreserve(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket(
            $this->getResourceName(),
            'in',
            'description',
            'bigquery',
        );
        $fs = new Filesystem();
        $fs->dumpFile(
            $this->getTmpDirPath() . '/data.csv',
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $csv = new CsvFile($this->getTmpDirPath() . '/data.csv');
        $this->clientWrapper->getBasicClient()->createTableAsync($bucketId, 'test', $csv);

        $component = new ComponentSpecification([
            'id' => self::COMPONENT_ID,
            'data' => [
                'definition' => [
                    'type' => 'aws-ecr',
                    // phpcs:ignore Generic.Files.LineLength.MaxExceeded
                    'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-workspace-test',
                    'tag' => '1.7.1',
                ],
                'staging-storage' => [
                    'input' => 'workspace-bigquery',
                    'output' => 'workspace-bigquery',
                ],
            ],
        ]);

        $jobConfiguration = new JobConfiguration(
            storage: new Storage(
                input: new Input(
                    tables: new TablesList([
                        [
                            'source' => sprintf('%s.test', $bucketId),
                            'destination' => 'test',
                            'keep_internal_timestamp_column' => false,
                        ],
                    ]),
                ),
            ),
        );
        $configuration = new Configuration();
        $configuration->setName('testWorkspaceBigQueryPreserve');
        $configuration->setComponentId(self::COMPONENT_ID);
        $configuration->setConfiguration($jobConfiguration->toArray());
        $componentsApi = new Components($this->clientWrapper->getBasicClient());
        $configId = $componentsApi->addConfiguration($configuration)['id'];

        // create bigquery workspace and load a table into it
        $workspace = $componentsApi->createConfigurationWorkspace(
            self::COMPONENT_ID,
            $configId,
            ['backend' => 'bigquery'],
            true,
        );
        $workspaceApi = new Workspaces($this->clientWrapper->getBasicClient());
        $workspaceApi->loadWorkspaceData(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => sprintf('%s.test', $bucketId),
                        'destination' => 'original',
                        'useView' => true,
                    ],
                ],
            ],
        );

        $dataLoader = $this->getInputDataLoader(
            component: $component,
            clientWrapper: $this->clientWrapper,
            configId: $configId,
        );
        $dataLoader->loadInputData(
            component: $component,
            jobConfiguration: $jobConfiguration,
            jobState: new State(),
        );
        $credentials = $dataLoader->getWorkspaceCredentials();
        self::assertEquals(['schema', 'region', 'credentials'], array_keys($credentials));
        self::assertNotEmpty($credentials['credentials']);

        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId(self::COMPONENT_ID)
                ->setConfigurationId($configId),
        );
        // workspace is not reused so another one was created
        self::assertCount(2, $workspaces);

        // the workspace is not reused and not the same
        self::assertNotSame($workspace['connection']['schema'], $credentials['schema']);

        // but the original table does exists (workspace was not cleared)
        try {
            $this->clientWrapper->getBasicClient()->writeTableAsyncDirect(
                sprintf('%s.test', $bucketId),
                ['dataWorkspaceId' => $workspaces[0]['id'], 'dataTableName' => 'original'],
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString(
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
                'Invalid columns: _timestamp: Only alphanumeric characters and underscores are allowed in column name. Underscore is not allowed on the beginning',
                $e->getMessage(),
            );
        }

        try {
            // the loaded table exists, but can not be loaded because of _timestamp column
            $this->clientWrapper->getBasicClient()->writeTableAsyncDirect(
                sprintf('%s.test', $bucketId),
                ['dataWorkspaceId' => $workspaces[0]['id'], 'dataTableName' => 'original'],
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString(
            // phpcs:ignore Generic.Files.LineLength.MaxExceeded
                'Invalid columns: _timestamp: Only alphanumeric characters and underscores are allowed in column name. Underscore is not allowed on the beginning',
                $e->getMessage(),
            );
        }

        $workspaceApi->deleteWorkspace($workspace['id'], async: true);
    }
}
