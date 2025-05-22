<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\Mapping\DataLoader\OutputDataLoader;
use Keboola\JobQueue\JobConfiguration\Mapping\DataLoader\OutputDataLoaderFactory;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\BaseDataLoaderTestCase;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StagingProvider\Workspace\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
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
        Configuration $config,
        ComponentSpecification $component,
        ?ClientWrapper $clientWrapper = null,
        ?string $configId = 'testConfig',
        ?string $configRowId = null,
    ): OutputDataLoader {
        $clientWrapper ??= $this->clientWrapper;

        $workspaceProvider = new WorkspaceProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator()),
        );

        $dataLoaderFactory = new OutputDataLoaderFactory(
            $workspaceProvider,
            new NullLogger(),
            $this->getDataDirPath(),
        );

        return $dataLoaderFactory->createOutputDataLoader(
            $clientWrapper,
            $component,
            $config,
            $configId,
            $configRowId,
            stagingWorkspaceId: null, // TODO
        );
    }
}
