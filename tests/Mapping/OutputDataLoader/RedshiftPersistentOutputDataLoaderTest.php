<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use ColinODell\PsrTestLogger\TestLogger;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration as JobConfiguration;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;

class RedshiftPersistentOutputDataLoaderTest extends BaseOutputDataLoaderTestCase
{
    protected const COMPONENT_ID = 'keboola.runner-config-test';
    protected const DEFAULT_PROJECT = 'redshift';

    public function setUp(): void
    {
        parent::setUp();
    }

    protected static function expectedDefaultTableBackend(): string
    {
        return 'redshift';
    }

    public function testRedshiftWorkspaceNoConfig(): void
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
                    'input' => 'workspace-redshift',
                    'output' => 'workspace-redshift',
                ],
            ],
        ]);
        $client = $this->getMockBuilder(BranchAwareClient::class)
            ->setConstructorArgs([
                'default',
                [
                    'url' => getenv('STORAGE_API_URL__REDSHIFT'),
                    'token' => getenv('TEST_STORAGE_API_TOKEN_REDSHIFT'),
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
            tokenInfo: ['owner' => ['hasRedshift' => true, 'features' => []]],
            tokenValue: 'token',
        ));
        $logger = new TestLogger;

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
            jobConfiguration: new JobConfiguration,
            branchId: null,
            runId: null,
            configId: null,
            configRowId: null,
        );

        $credentials = $dataLoader->getWorkspaceCredentials();
        self::assertEquals(
            ['host', 'warehouse', 'database', 'schema', 'user', 'password'],
            array_keys($credentials),
        );
        self::assertStringEndsWith('redshift.amazonaws.com', $credentials['host']);
        self::assertTrue($logger->hasNoticeThatContains('Created a new ephemeral workspace.'));

        $this->getWorkspaceCleaner(
            clientWrapper: $clientWrapper,
            configId: null,
            component: $component,
            workspaceProvider: $workspaceProvider,
        )->cleanWorkspace($component, configId: null);
        // checked in mock that the workspace is deleted
    }

    public function testRedshiftWorkspaceConfigNoWorkspace(): void
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
                    'input' => 'workspace-redshift',
                    'output' => 'workspace-redshift',
                ],
            ],
        ]);
        $client = $this->getMockBuilder(BranchAwareClient::class)
            ->setConstructorArgs([
                'default',
                [
                    'url' => getenv('STORAGE_API_URL__REDSHIFT'),
                    'token' => getenv('TEST_STORAGE_API_TOKEN_REDSHIFT'),
                ],
            ])
            ->onlyMethods(['apiDelete'])
            ->getMock();
        $client->expects(self::never())->method('apiDelete');
        $configuration = new Configuration();
        $configuration->setName('test-dataloader');
        $configuration->setComponentId(self::COMPONENT_ID);
        $configuration->setConfiguration([]);
        $componentsApi = new Components($this->clientWrapper->getBasicClient());
        $configurationId = $componentsApi->addConfiguration($configuration)['id'];
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);
        $clientWrapper->method('getBranchClient')->willReturn($client);
        $clientWrapper->method('getToken')->willReturn(new StorageApiToken(
            tokenInfo: ['owner' => ['hasRedshift' => true, 'features' => []]],
            tokenValue: 'token',
        ));
        $logger = new TestLogger;

        $dataLoader = $this->getOutputDataLoader(
            component: $component,
            clientWrapper: $this->clientWrapper,
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
        self::assertEquals(
            ['host', 'warehouse', 'database', 'schema', 'user', 'password'],
            array_keys($credentials),
        );
        self::assertStringEndsWith('redshift.amazonaws.com', $credentials['host']);
        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId(self::COMPONENT_ID)
                ->setConfigurationId($configurationId),
        );
        self::assertCount(1, $workspaces);
        $this->getWorkspaceCleaner(
            clientWrapper: $clientWrapper,
            configId: $configurationId,
            component: $component,
            logger: $logger,
        )->cleanWorkspace($component, $configurationId);
        // double check that workspace still exists
        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId(self::COMPONENT_ID)
                ->setConfigurationId($configurationId),
        );
        self::assertCount(1, $workspaces);
        // cleanup after the test
        $workspacesApi = new Workspaces($this->clientWrapper->getBasicClient());
        $workspacesApi->deleteWorkspace($workspaces[0]['id'], [], true);
        self::assertTrue($logger->hasInfoThatContains('Created a new persistent workspace'));
    }

    public function testRedshiftWorkspaceConfigOneWorkspace(): void
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
                    'input' => 'workspace-redshift',
                    'output' => 'workspace-redshift',
                ],
            ],
        ]);
        $client = $this->getMockBuilder(BranchAwareClient::class)
            ->setConstructorArgs([
                'default',
                [
                    'url' => getenv('STORAGE_API_URL__REDSHIFT'),
                    'token' => getenv('TEST_STORAGE_API_TOKEN_REDSHIFT'),
                ],
            ])
            ->onlyMethods(['apiDelete'])
            ->getMock();
        $client->expects(self::never())->method('apiDelete');
        $configuration = new Configuration();
        $configuration->setName('test-dataloader');
        $configuration->setComponentId(self::COMPONENT_ID);
        $configuration->setConfiguration([]);
        $componentsApi = new Components($this->clientWrapper->getBasicClient());
        $configurationId = $componentsApi->addConfiguration($configuration)['id'];
        $workspace = $componentsApi->createConfigurationWorkspace(
            self::COMPONENT_ID,
            $configurationId,
            ['backend' => 'redshift'],
            true,
        );
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);
        $clientWrapper->method('getBranchClient')->willReturn($client);
        $logger = new TestLogger;

        $dataLoader = $this->getOutputDataLoader(
            component: $component,
            clientWrapper: $this->clientWrapper,
            logger: $logger,
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
        self::assertEquals(
            ['host', 'warehouse', 'database', 'schema', 'user', 'password'],
            array_keys($credentials),
        );
        self::assertStringEndsWith('redshift.amazonaws.com', $credentials['host']);
        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId(self::COMPONENT_ID)
                ->setConfigurationId($configurationId),
        );
        self::assertCount(1, $workspaces);
        $this->getWorkspaceCleaner(
            clientWrapper: $this->clientWrapper,
            configId: $configurationId,
            component: $component,
            logger: $logger,
        )->cleanWorkspace($component, $configurationId);

        // double check that workspace still exists
        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId(self::COMPONENT_ID)
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

    public function testRedshiftWorkspaceConfigMultipleWorkspace(): void
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
                    'input' => 'workspace-redshift',
                    'output' => 'workspace-redshift',
                ],
            ],
        ]);
        $client = $this->getMockBuilder(BranchAwareClient::class)
            ->setConstructorArgs([
                'default',
                [
                    'url' => getenv('STORAGE_API_URL__REDSHIFT'),
                    'token' => getenv('TEST_STORAGE_API_TOKEN_REDSHIFT'),
                ],
            ])
            ->onlyMethods(['apiDelete'])
            ->getMock();
        $client->expects(self::never())->method('apiDelete');
        $configuration = new Configuration();
        $configuration->setConfiguration([]);
        $configuration->setName('test-dataloader');
        $configuration->setComponentId(self::COMPONENT_ID);
        $componentsApi = new Components($this->clientWrapper->getBasicClient());
        $configurationId = $componentsApi->addConfiguration($configuration)['id'];
        $workspace1 = $componentsApi->createConfigurationWorkspace(
            self::COMPONENT_ID,
            $configurationId,
            ['backend' => 'redshift'],
            true,
        );
        $workspace2 = $componentsApi->createConfigurationWorkspace(
            self::COMPONENT_ID,
            $configurationId,
            ['backend' => 'redshift'],
            true,
        );

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);
        $clientWrapper->method('getBranchClient')->willReturn($client);
        $logger = new TestLogger;

        $dataLoader = $this->getOutputDataLoader(
            component: $component,
            clientWrapper: $this->clientWrapper,
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

        $this->assertTrue($logger->hasWarning(
            sprintf(
                'Multiple workspaces (total 2) found (IDs: %s) for configuration "%s" of component "%s", using "%s".',
                implode(',', [$workspace1['id'], $workspace2['id']]),
                $configurationId,
                'keboola.runner-config-test',
                $workspace1['id'],
            ),
        ));
        $credentials = $dataLoader->getWorkspaceCredentials();
        self::assertEquals(
            ['host', 'warehouse', 'database', 'schema', 'user', 'password'],
            array_keys($credentials),
        );
        self::assertStringEndsWith('redshift.amazonaws.com', $credentials['host']);
        $workspaces = $componentsApi->listConfigurationWorkspaces(
            (new ListConfigurationWorkspacesOptions())
                ->setComponentId(self::COMPONENT_ID)
                ->setConfigurationId($configurationId),
        );
        self::assertCount(2, $workspaces);

        $workspacesApi = new Workspaces($this->clientWrapper->getBasicClient());
        $workspacesApi->deleteWorkspace($workspace1['id'], [], true);
        $workspacesApi->deleteWorkspace($workspace2['id'], [], true);
    }
}
