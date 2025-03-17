<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Backend;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials;
use Keboola\StagingProvider\Provider\NewWorkspaceStagingProvider;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;

class WorkspaceCleanerTest extends BaseDataLoaderTestCase
{
    protected static function expectedDefaultTableBackend(): string
    {
        return 'snowflake';
    }

    public function testWorkspaceCleanupSuccess(): void
    {
        $componentsApiClient = new Components($this->clientWrapper->getBasicClient());

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

        $configId = $this->createConfig($componentId, 'test-workspaceCleaner-cleanupSuccess');

        $workspaceListOptions = new ListConfigurationWorkspacesOptions();
        $workspaceListOptions->setComponentId($componentId)->setConfigurationId($configId);

        $clientMock = $this->createMock(BranchAwareClient::class);
        $clientMock->method('verifyToken')->willReturn($this->clientWrapper->getBasicClient()->verifyToken());
        $clientMock->expects(self::never())->method('apiPostJson');
        $clientMock->expects(self::never())->method('apiDelete');

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBasicClient')->willReturn($clientMock);
        $clientWrapperMock->method('getBranchClient')->willReturn($clientMock);

        self::assertCount(0, $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions));

        $workspaceCleaner = $this->getWorkspaceCleaner(
            clientWrapper: $clientWrapperMock,
            configId: $configId,
            component: $component,
        );

        // immediately calling cleanWorkspace without using it means it was not initialized
        $workspaceCleaner->cleanWorkspace($component, $configId);

        self::assertCount(0, $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions));
    }

    public function testWorkspaceCleanupWhenInitialized(): void
    {
        $componentsApiClient = new Components($this->clientWrapper->getBasicClient());

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

        $configId = $this->createConfig($componentId, 'test-workspaceCleaner-cleanupWhenInitialized');

        $workspaceListOptions = new ListConfigurationWorkspacesOptions();
        $workspaceListOptions->setComponentId($componentId)->setConfigurationId($configId);

        $logger = new NullLogger();

        // create InputStrategyFactory manually so we can trigger workspace creation
        $workspaceProviderFactory = $this->createWorkspaceProvider(
            $component,
            $configId,
        );

        $inputStrategyFactory = $this->createInputStrategyFactory(
            $component,
            $workspaceProviderFactory,
        );

        // this causes workspace creation (so that we can test its cleanup)
        $tableDataProvider = $inputStrategyFactory->getStrategyMap()['workspace-snowflake']->getTableDataProvider();
        self::assertInstanceOf(NewWorkspaceStagingProvider::class, $tableDataProvider);
        $tableDataProvider->getCredentials();
        self::assertCount(1, $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions));

        $workspaceCleaner = $this->getWorkspaceCleaner(
            clientWrapper: $this->clientWrapper,
            configId: $configId,
            component: $component,
            workspaceProvider: $workspaceProviderFactory,
            inputStrategyFactory: $inputStrategyFactory,
            logger: $logger,
        );

        $workspaceCleaner->cleanWorkspace(
            $component,
            $configId,
        );

        self::assertCount(0, $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions));
    }


    public static function workspaceCleanupFailureProvider(): iterable
    {
        yield 'Bad request' => [
            'deleteException' => new ClientException('Bad request', 400),
            'shouldBeLogged' => true,
        ];

        yield 'Not found' => [
            'deleteException' => new ClientException('Workspace not found', 404),
            'shouldBeLogged' => false,
        ];

        yield 'Unauthorized' => [
            'deleteException' => new ClientException('Unauthorized', 401),
            'shouldBeLogged' => true,
        ];
    }

    /** @dataProvider workspaceCleanupFailureProvider */
    public function testWorkspaceCleanupFailure(
        ClientException $deleteException,
        bool $shouldBeLogged,
    ): void {
        $componentsApiClient = new Components($this->clientWrapper->getBasicClient());

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

        $storageApiToken = $this->createMock(BranchAwareClient::class);
        $storageApiToken->method('verifyToken')->willReturn(
            $this->clientWrapper->getBasicClient()->verifyToken(),
        );
        $storageApiToken->method('apiPostJson')->willReturnCallback(
            $this->clientWrapper->getBasicClient()->apiPostJson(...),
        );

        // simulate API error
        $storageApiToken->expects(self::once())
            ->method('apiDelete')
            ->willThrowException($deleteException)
        ;

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getToken')->willReturn($this->clientWrapper->getToken());
        $clientWrapper->method('getBasicClient')->willReturn($storageApiToken);
        $clientWrapper->method('getBranchClient')->willReturn($storageApiToken);

        $configId = $this->createConfig($componentId, 'test-workspaceCleaner-cleanupFailure');

        $workspaceListOptions = new ListConfigurationWorkspacesOptions();
        $workspaceListOptions->setComponentId($componentId)->setConfigurationId($configId);

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        // create InputStrategyFactory manually so we can trigger workspace creation
        $workspaceProviderFactory = $this->createWorkspaceProvider(
            $component,
            $configId,
            clientWrapper: $clientWrapper,
        );

        $inputStrategyFactory = $this->createInputStrategyFactory(
            $component,
            $workspaceProviderFactory,
            clientWrapper: $clientWrapper,
        );

        // this causes workspace creation (so that we can test its cleanup)
        $tableDataProvider = $inputStrategyFactory->getStrategyMap()['workspace-snowflake']->getTableDataProvider();
        self::assertInstanceOf(NewWorkspaceStagingProvider::class, $tableDataProvider);
        $tableDataProvider->getCredentials();
        self::assertCount(1, $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions));

        $workspaceCleaner = $this->getWorkspaceCleaner(
            clientWrapper: $clientWrapper,
            configId: $configId,
            component: $component,
            workspaceProvider: $workspaceProviderFactory,
            inputStrategyFactory: $inputStrategyFactory,
            logger: $logger,
        );

        $workspaceCleaner->cleanWorkspace(
            $component,
            $configId,
        );

        if ($shouldBeLogged) {
            self::assertTrue($logsHandler->hasErrorThatContains(
                'Failed to cleanup workspace: ' . $deleteException->getMessage(),
            ));
        } else {
            self::assertFalse($logsHandler->hasRecords(LogLevel::ERROR));
        }
    }

    public function testExternallyManagedWorkspaceSuccess(): void
    {
        $componentsApiClient = new Components($this->clientWrapper->getBasicClient());

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

        $configId = $this->createConfig($componentId, 'test-workspaceCleaner-externallyManagedWorkspaceSuccess');

        $workspaceListOptions = new ListConfigurationWorkspacesOptions();
        $workspaceListOptions->setComponentId($componentId)->setConfigurationId($configId);

        // create InputStrategyFactory manually so we can trigger workspace creation
        $workspaceProviderPrev = $this->createWorkspaceProvider(
            $component,
            $configId,
        );

        $inputStrategyFactory = $this->createInputStrategyFactory(
            $component,
            $workspaceProviderPrev,
        );

        $tableDataProvider = $inputStrategyFactory->getStrategyMap()['workspace-snowflake']->getTableDataProvider();
        self::assertInstanceOf(NewWorkspaceStagingProvider::class, $tableDataProvider);
        $credentials = $tableDataProvider->getCredentials();
        self::assertCount(1, $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions));

        $workspaceCredentials = WorkspaceCredentials::fromArray([
            'id' => $tableDataProvider->getWorkspaceId(),
            'type' => $tableDataProvider->getBackendType(),
            '#password' => $credentials['password'],
        ]);

        // create fresh workspaceProvider unaware of the existing workspace
        $workspaceProviderCurrent = $this->createWorkspaceProvider(
            component: $component,
            configId: $configId,
            backendConfig: new Backend(
                workspaceCredentials: $workspaceCredentials,
            ),
        );

        $workspaceCleaner = $this->getWorkspaceCleaner(
            clientWrapper: $this->clientWrapper,
            configId: $configId,
            component: $component,
            workspaceProvider: $workspaceProviderCurrent,
        );

        $workspaceCleaner->cleanWorkspace(
            $component,
            $configId,
        );

        // cleanWorkspace should not delete workspace if credentials were provided manually
        $existingWorkspaces = $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions);
        self::assertCount(1, $existingWorkspaces);
        self::assertSame($workspaceCredentials->id, (string) $existingWorkspaces[0]['id']);
    }

    /**
     * @param non-empty-string $configId
     * @return non-empty-string
     */
    private function createConfig(
        string $componentId,
        string $configId,
        array $configuration = [],
        ?StorageApiClient $storageApiClient = null,
    ): string {
        $storageApiClient ??= $this->clientWrapper->getBranchClient();
        $componentsApiClient = new Components($storageApiClient);
        $workspacesApi = new Workspaces($storageApiClient);

        $workspaceListOptions = (new ListConfigurationWorkspacesOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configId)
        ;
        try {
            $existingWorkspaces = $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }

            $existingWorkspaces = [];
        }
        foreach ($existingWorkspaces as $workspace) {
            $workspacesApi->deleteWorkspace($workspace['id'], async: true);
        }

        try {
            $componentsApiClient->deleteConfiguration($componentId, $configId);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        $config = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configId)
            ->setName($configId)
            ->setConfiguration($configuration)
        ;

        $componentsApiClient->addConfiguration($config);

        return $configId;
    }
}
