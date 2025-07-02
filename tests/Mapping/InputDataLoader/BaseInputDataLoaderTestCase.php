<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Keboola\JobQueue\JobConfiguration\Mapping\DataLoader\InputDataLoader;
use Keboola\JobQueue\JobConfiguration\Mapping\StagingWorkspace\StagingWorkspaceFacade;
use Keboola\JobQueue\JobConfiguration\Mapping\StagingWorkspace\StagingWorkspaceFactory;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\BaseDataLoaderTestCase;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Workspace\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;
use Psr\Log\NullLogger;

abstract class BaseInputDataLoaderTestCase extends BaseDataLoaderTestCase
{
    protected static function expectedDefaultTableBackend(): string
    {
        return 'snowflake';
    }

    protected function getInputDataLoader(
        ComponentSpecification $component,
        Configuration $config,
        State $state = new State(),
        ?string $stagingWorkspaceId = null,
        ?ClientWrapper $clientWrapper = null,
    ): InputDataLoader {
        $inputDataLoader = InputDataLoader::create(
            new NullLogger(),
            $clientWrapper ?? $this->clientWrapper,
            $component,
            $config,
            $state,
            $stagingWorkspaceId,
            $this->getDataDirPath(),
            'in/',
        );
        assert($inputDataLoader !== null);

        return $inputDataLoader;
    }

    protected function getStagingWorkspaceFacade(
        StorageApiToken $storageApiToken,
        ComponentSpecification $component,
        Configuration $configuration = new Configuration(),
        ?string $configId = null,
        ?ClientWrapper $clientWrapper = null,
    ): StagingWorkspaceFacade {
        $clientWrapper ??= $this->clientWrapper;

        // ensure we're dealing with a component with workspace staging
        self::assertSame(
            StagingClass::Workspace,
            StagingType::from($component->getInputStagingStorage())->getStagingClass(),
        );

        $workspaceProvider = new WorkspaceProvider(
            new Workspaces($clientWrapper->getBranchClient()),
            new Components($clientWrapper->getBranchClient()),
            new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator()),
        );

        $stagingWorkspaceFactory = new StagingWorkspaceFactory(
            $workspaceProvider,
            new NullLogger(),
        );

        $stagingWorkspaceFacade = $stagingWorkspaceFactory->createStagingWorkspaceFacade(
            $storageApiToken,
            $component,
            $configuration,
            $configId,
        );

        // factory always returns staging for components with workspace staging
        assert($stagingWorkspaceFacade !== null);

        return $stagingWorkspaceFacade;
    }
}
