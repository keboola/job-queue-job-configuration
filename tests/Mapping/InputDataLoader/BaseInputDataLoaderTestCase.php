<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\Mapping\InputDataLoader;
use Keboola\JobQueue\JobConfiguration\Mapping\WorkspaceProviderFactory;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\BaseDataLoaderTestCase;
use Keboola\StagingProvider\InputProviderInitializer;
use Keboola\StagingProvider\Provider\LocalStagingProvider;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;

abstract class BaseInputDataLoaderTestCase extends BaseDataLoaderTestCase
{
    protected static function expectedDefaultTableBackend(): string
    {
        return 'snowflake';
    }

    protected function getInputDataLoader(
        ComponentSpecification $component,
        ?ClientWrapper $clientWrapper = null,
        ?string $configId = null,
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

        $workspaceProviderFactoryFactory = new WorkspaceProviderFactory(
            componentsApiClient: $componentsApi,
            workspacesApiClient: $workspacesApi,
            logger: $logger,
        );

        assert($configId !== '');
        $workspaceProvider = $workspaceProviderFactoryFactory->getWorkspaceStaging(
            stagingStorage: $component->getInputStagingStorage(),
            component: $component,
            configId: $configId,
            backendConfig: null,
            useReadonlyRole: null,
        );

        $inputProviderInitializer = new InputProviderInitializer(
            stagingFactory: $inputStrategyFactory,
            workspaceStagingProvider: $workspaceProvider,
            localStagingProvider: new LocalStagingProvider($this->getWorkingDirPath()),
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
