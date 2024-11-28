<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\JobQueue\JobConfiguration\Mapping\InputDataLoader;
use Keboola\JobQueue\JobConfiguration\Mapping\WorkspaceProviderFactoryFactory;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\BaseDataLoaderTestCase;
use Keboola\StagingProvider\InputProviderInitializer;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class BaseInputDataLoaderTestCase extends BaseDataLoaderTestCase
{
    protected function getInputDataLoader(
        ?ClientWrapper $clientWrapper = null,
        ?string $configId = null,
        ?string $componentStagingStorageType = null,
    ): InputDataLoader {
        $clientWrapper = $clientWrapper ?? $this->clientWrapper;
        $logger = new NullLogger();

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

        assert($configId !== '');
        $workspaceProviderFactory = $workspaceProviderFactoryFactory->getWorkspaceProviderFactory(
            stagingStorage: $component->getInputStagingStorage(),
            component: $component,
            configId: $configId,
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
