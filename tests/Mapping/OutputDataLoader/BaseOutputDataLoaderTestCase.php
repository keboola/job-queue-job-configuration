<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\Mapping\OutputDataLoader;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\BaseDataLoaderTestCase;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StagingProvider\Provider\AbstractWorkspaceProvider;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class BaseOutputDataLoaderTestCase extends BaseDataLoaderTestCase
{
    protected static function expectedDefaultTableBackend(): string
    {
        return 'snowflake';
    }

    protected function getOutputDataLoader(
        ComponentSpecification $component,
        ?ClientWrapper $clientWrapper = null,
        ?AbstractWorkspaceProvider $workspaceProvider = null,
        ?OutputStrategyFactory $outputStrategyFactory = null,
        LoggerInterface $logger = new NullLogger(),
        ?string $configId = null,
        ?bool $readOnlyWorkspace = null,
    ): OutputDataLoader {
        $clientWrapper = $clientWrapper ?? $this->clientWrapper;

        $workspaceProvider ??= $this->createWorkspaceProvider(
            component: $component,
            configId: $configId,
            readOnlyWorkspace: $readOnlyWorkspace,
            clientWrapper: $clientWrapper,
            logger: $logger,
        );

        $outputStrategyFactory ??= $this->createOutputStrategyFactory(
            component: $component,
            clientWrapper: $clientWrapper,
            workspaceProvider: $workspaceProvider,
            logger: $logger,
        );

        return new OutputDataLoader(
            outputStrategyFactory: $outputStrategyFactory,
            logger: $logger,
            dataOutDir: '/data/out',
        );
    }
}
