<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use ColinODell\PsrTestLogger\TestLogger;
use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration as JobConfiguration;
use Keboola\JobQueue\JobConfiguration\Mapping\WorkspaceProviderFactory;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;

class ABSPersistentOutputDataLoaderTest extends BaseOutputDataLoaderTestCase
{
    protected const COMPONENT_ID = 'keboola.runner-config-test';
    protected const DEFAULT_PROJECT = 'azure';

    protected function setUp(): void
    {
        parent::setUp();

        $this->assertFileBackend('azure', $this->clientWrapper->getBasicClient());
    }

    protected static function expectedDefaultTableBackend(): string
    {
        return 'snowflake';
    }

    public function testAbsWorkspaceNoConfig(): void
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
                    'input' => 'workspace-abs',
                    'output' => 'workspace-abs',
                ],
            ],
        ]);
        $client = $this->getMockBuilder(BranchAwareClient::class)
            ->setConstructorArgs([
                'default',
                [
                    'url' => getenv('STORAGE_API_URL_AZURE'),
                    'token' => getenv('TEST_STORAGE_API_TOKEN_MASTER_AZURE'),
                ],
            ])
            ->onlyMethods(['apiDelete'])
            ->getMock();
        $client->expects($this->once())->method('apiDelete')->with(self::stringContains('workspaces/'));
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);
        $clientWrapper->method('getBranchClient')->willReturn($client);
        $clientWrapper->method('getToken')->willReturn(new StorageApiToken(
            tokenInfo: ['owner' => ['fileStorageProvider' => 'azure', 'features' => []]],
            tokenValue: 'token',
        ));
        $logger = new TestLogger();

        // workspaceProvider holds the workspace reference so it must be shared between dataLoader & workspaceCleaner
        $workspaceProvider = $this->createWorkspaceProvider(
            component: $component,
            configId: null,
            clientWrapper: $clientWrapper,
            logger: $logger,
        );

        $dataLoader = $this->getOutputDataLoader(
            component: $component,
            clientWrapper: $clientWrapper,
            workspaceProvider: $workspaceProvider,
            logger: $logger,
        );
        $dataLoader->storeOutput(
            component: $component,
            jobConfiguration: new JobConfiguration(),
            branchId: null,
            runId: null,
            configId: null,
            configRowId: null,
        );

        $credentials = $dataLoader->getWorkspaceCredentials();
        self::assertEquals(['container', 'connectionString'], array_keys($credentials));
        self::assertStringStartsWith('BlobEndpoint=https://', $credentials['connectionString']);
        self::assertTrue($logger->hasNoticeThatContains('Creating a new ephemeral workspace.'));

        $this->getWorkspaceCleaner(
            clientWrapper: $clientWrapper,
            configId: null,
            component: $component,
            workspaceProvider: $workspaceProvider,
        )->cleanWorkspace($component, configId: null);

        // checked in mock that the workspace is deleted
    }

    public function testAbsWorkspaceConfigNoWorkspace(): void
    {
        $component = new ComponentSpecification([
            'id' => 'keboola.runner-config-test',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'workspace-abs',
                    'output' => 'workspace-abs',
                ],
            ],
        ]);
        $client = $this->getMockBuilder(BranchAwareClient::class)
            ->setConstructorArgs([
                'default',
                [
                    'url' => getenv('STORAGE_API_URL_AZURE'),
                    'token' => getenv('TEST_STORAGE_API_TOKEN_MASTER_AZURE'),
                ],
            ])
            ->onlyMethods(['apiDelete'])
            ->getMock();
        $client->expects(self::never())->method('apiDelete');

        $configuration = new Configuration();
        $configuration->setConfiguration([]);
        $configuration->setName('test-dataloader');
        $configuration->setComponentId('keboola.runner-config-test');

        $componentsApi = new Components($this->clientWrapper->getBasicClient());
        $configurationId = $componentsApi->addConfiguration($configuration)['id'];
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);
        $clientWrapper->method('getBranchClient')->willReturn($client);
        $clientWrapper->method('getToken')->willReturn(new StorageApiToken(
            ['owner' => ['fileStorageProvider' => 'azure', 'features' => []]],
            'token',
        ));

        $logger = new TestLogger();
        $dataLoader = $this->getOutputDataLoader(
            component: $component,
            clientWrapper: $clientWrapper,
            logger: $logger,
            configId: $configurationId,
        );
        $dataLoader->storeOutput(
            component: $component,
            jobConfiguration: new JobConfiguration(),
            branchId: null,
            runId: null,
            configId: $configurationId,
            configRowId: null,
        );

        $credentials = $dataLoader->getWorkspaceCredentials();
        self::assertEquals(['container', 'connectionString'], array_keys($credentials));
        self::assertStringStartsWith('BlobEndpoint=https://', $credentials['connectionString']);

        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId('keboola.runner-config-test')
                ->setConfigurationId($configurationId),
        );
        self::assertCount(1, $workspaces);

        $this->getWorkspaceCleaner(
            clientWrapper: $clientWrapper,
            configId: $configurationId,
            component: $component,
        )->cleanWorkspace($component, $configurationId);

        // double check that workspace still exists
        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId('keboola.runner-config-test')
                ->setConfigurationId($configurationId),
        );
        self::assertCount(1, $workspaces);
        // cleanup after the test
        $workspacesApi = new Workspaces($this->clientWrapper->getBasicClient());
        $workspacesApi->deleteWorkspace($workspaces[0]['id'], [], true);
        self::assertTrue($logger->hasInfoThatContains('Creating a new persistent workspace'));
    }

    public function testAbsWorkspaceConfigOneWorkspace(): void
    {
        $component = new ComponentSpecification([
            'id' => 'keboola.runner-config-test',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'workspace-abs',
                    'output' => 'workspace-abs',
                ],
            ],
        ]);
        $client = $this->getMockBuilder(BranchAwareClient::class)
            ->setConstructorArgs([
                'default',
                [
                    'url' => getenv('STORAGE_API_URL_AZURE'),
                    'token' => getenv('TEST_STORAGE_API_TOKEN_MASTER_AZURE'),
                ],
            ])
            ->onlyMethods(['apiDelete'])
            ->getMock();
        $client->expects(self::never())->method('apiDelete');
        $configuration = new Configuration();
        $configuration->setConfiguration([]);
        $configuration->setName('test-dataloader');
        $configuration->setComponentId('keboola.runner-config-test');
        $componentsApi = new Components($client);
        $configurationId = $componentsApi->addConfiguration($configuration)['id'];

        $workspace = $componentsApi->createConfigurationWorkspace(
            'keboola.runner-config-test',
            $configurationId,
            ['backend' => 'abs'],
            true,
        );
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);
        $clientWrapper->method('getBranchClient')->willReturn($client);
        $clientWrapper->method('getToken')->willReturn(new StorageApiToken(
            tokenInfo: ['owner' => ['fileStorageProvider' => 'azure', 'features' => []]],
            tokenValue: 'token',
        ));
        $logger = new TestLogger();

        $dataLoader = $this->getOutputDataLoader(
            component: $component,
            clientWrapper: $clientWrapper,
            logger: $logger,
            configId: $configurationId,
        );
        $dataLoader->storeOutput(
            component: $component,
            jobConfiguration: new JobConfiguration(),
            branchId: null,
            runId: null,
            configId: $configurationId,
            configRowId: null,
        );

        $credentials = $dataLoader->getWorkspaceCredentials();
        self::assertEquals(['container', 'connectionString'], array_keys($credentials));
        self::assertStringStartsWith('BlobEndpoint=https://', $credentials['connectionString']);
        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId('keboola.runner-config-test')
                ->setConfigurationId($configurationId),
        );
        self::assertCount(1, $workspaces);

        $this->getWorkspaceCleaner(
            clientWrapper: $clientWrapper,
            configId: $configurationId,
            component: $component,
        )->cleanWorkspace($component, $configurationId);

        // double check that workspace still exists
        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId('keboola.runner-config-test')
                ->setConfigurationId($configurationId),
        );
        self::assertCount(1, $workspaces);
        // and it must be the same workspace we created beforehand
        self::assertEquals($workspace['id'], $workspaces[0]['id']);
        // cleanup after the test
        $workspacesApi = new Workspaces($this->clientWrapper->getBasicClient());
        $workspacesApi->deleteWorkspace($workspaces[0]['id'], [], true);
        self::assertTrue($logger->hasInfoThatContains(
            sprintf('Reusing persistent workspace "%s".', $workspace['id']),
        ));
    }

    public function testAbsWorkspaceConfigMultipleWorkspace(): void
    {
        $component = new ComponentSpecification([
            'id' => 'keboola.runner-config-test',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 'workspace-abs',
                    'output' => 'workspace-abs',
                ],
            ],
        ]);
        $client = $this->getMockBuilder(BranchAwareClient::class)
            ->setConstructorArgs([
                'default',
                [
                    'url' => getenv('STORAGE_API_URL_AZURE'),
                    'token' => getenv('TEST_STORAGE_API_TOKEN_MASTER_AZURE'),
                ],
            ])
            ->onlyMethods(['apiDelete'])
            ->getMock();
        $client->expects(self::never())->method('apiDelete');

        $configuration = new Configuration();
        $configuration->setConfiguration([]);
        $configuration->setName('test-dataloader');
        $configuration->setComponentId('keboola.runner-config-test');
        $componentsApi = new Components($this->clientWrapper->getBasicClient());
        $configurationId = $componentsApi->addConfiguration($configuration)['id'];
        $workspace1 = $componentsApi->createConfigurationWorkspace(
            'keboola.runner-config-test',
            $configurationId,
            ['backend' => 'abs'],
            true,
        );
        $workspace2 = $componentsApi->createConfigurationWorkspace(
            'keboola.runner-config-test',
            $configurationId,
            ['backend' => 'abs'],
            true,
        );

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $clientWrapper->method('getBranchClient')->willReturn($client);
        $logger = new TestLogger();
        try {
            $componentsApi = new Components($clientWrapper->getBasicClient());
            $workspacesApi = new Workspaces($clientWrapper->getBasicClient());
            $snowflakeKeyPairGenerator = new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator());

            $workspaceFactory = new WorkspaceProviderFactory(
                $componentsApi,
                $workspacesApi,
                $snowflakeKeyPairGenerator,
                $logger,
            );
            $workspaceFactory->getWorkspaceStaging(
                'workspace-abs',
                $component,
                $configurationId,
                null,
                null,
            );
        } catch (ApplicationException $e) {
            self::assertEquals(
                sprintf(
                    'Multiple workspaces (total 2) found (IDs: %s, %s) for configuration "%s" of component "%s".',
                    $workspace1['id'],
                    $workspace2['id'],
                    $configurationId,
                    'keboola.runner-config-test',
                ),
                $e->getMessage(),
            );
            $workspacesApi = new Workspaces($this->clientWrapper->getBasicClient());
            $workspacesApi->deleteWorkspace($workspace1['id'], [], true);
            $workspacesApi->deleteWorkspace($workspace2['id'], [], true);
        }
    }
}
