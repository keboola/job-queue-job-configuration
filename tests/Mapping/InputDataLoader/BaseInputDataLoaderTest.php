<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\JobQueue\JobConfiguration\Mapping\InputDataLoader;
use Keboola\JobQueue\JobConfiguration\Mapping\WorkspaceProviderFactoryFactory;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\BaseDataLoaderTest;
use Keboola\StagingProvider\InputProviderInitializer;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class BaseInputDataLoaderTest extends BaseDataLoaderTest
{
    protected function getInputDataLoader(
        ?ClientWrapper $clientWrapper = null,
        LoggerInterface $logger = new NullLogger(),
        ?string $configId = null,
        ?string $componentStagingStorageType = null,
    ): InputDataLoader {
        $clientWrapper = $clientWrapper ?? $this->clientWrapper;

        $inputStrategyFactory = new StrategyFactory(
            clientWrapper: $clientWrapper,
            logger: $logger,
            format: 'json',
        );

        $componentsApi = new Components($clientWrapper->getBasicClient());
        $workspacesApi = new Workspaces($clientWrapper->getBasicClient());

        $component = $this->getComponentWithDefaultBucket($componentStagingStorageType);

        $workspaceProviderFactoryFactory = new WorkspaceProviderFactoryFactory(
            componentsApiClient: $componentsApi,
            workspacesApiClient: $workspacesApi,
            logger: $logger,
        );

        $workspaceProviderFactory = $workspaceProviderFactoryFactory->getWorkspaceProviderFactory(
            stagingStorage: $component->getInputStagingStorage(),
            component: $component,
            configId: $configId !== '' ? $configId : null,
            backendConfig: null,
            useReadonlyRole: null,
        );

        $inputProviderInitializer = new InputProviderInitializer(
            stagingFactory: $inputStrategyFactory,
            workspaceProviderFactory: $workspaceProviderFactory,
            dataDirectory: $this->getWorkingDirPath(),
        );

        $inputProviderInitializer->initializeProviders(
            stagingType: $component->getOutputStagingStorage(),
            tokenInfo: $clientWrapper->getToken()->getTokenInfo(),
        );

        return new InputDataLoader(
            inputStrategyFactory: $inputStrategyFactory,
            logger: $logger,
            dataInDir: '/data/in',
        );
    }
}
