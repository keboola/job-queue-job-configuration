<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Keboola\JobQueue\JobConfiguration\Mapping\OutputDataLoader;
use Keboola\JobQueue\JobConfiguration\Mapping\WorkspaceProviderFactoryFactory;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\BaseDataLoaderTest;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StagingProvider\OutputProviderInitializer;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;

abstract class BaseOutputDataLoaderTest extends BaseDataLoaderTest
{
    protected function getOutputDataLoader(?ClientWrapper $clientWrapper = null): OutputDataLoader {
        $clientWrapper = $clientWrapper ?? $this->clientWrapper;

        $outputStrategyFactory = new OutputStrategyFactory(
            clientWrapper: $clientWrapper,
            logger: new NullLogger(),
            format: 'json',
        );

        $componentsApi = new Components($clientWrapper->getBasicClient());
        $workspacesApi = new Workspaces($clientWrapper->getBasicClient());

        $component = $this->getComponentWithDefaultBucket();

        $workspaceProviderFactoryFactory = new WorkspaceProviderFactoryFactory(
            componentsApiClient: $componentsApi,
            workspacesApiClient: $workspacesApi,
            logger: new NullLogger(),
        );

        $workspaceProviderFactory = $workspaceProviderFactoryFactory->getWorkspaceProviderFactory(
            stagingStorage: $component->getInputStagingStorage(),
            component: $component,
            configId: null,
            backendConfig: null,
            useReadonlyRole: null,
        );

        $outputProviderInitializer = new OutputProviderInitializer(
            stagingFactory: $outputStrategyFactory,
            workspaceProviderFactory: $workspaceProviderFactory,
            dataDirectory: $this->getWorkingDirPath(),
        );

        $outputProviderInitializer->initializeProviders(
            stagingType: $component->getOutputStagingStorage(),
            tokenInfo: [],
        );

        return new OutputDataLoader(
            outputStrategyFactory: $outputStrategyFactory,
            logger: new NullLogger(),
            dataOutDir: '/data/out',
        );
    }
}
