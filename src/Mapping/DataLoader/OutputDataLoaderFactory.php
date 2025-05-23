<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping\DataLoader;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApiBranch\ClientWrapper;

class OutputDataLoaderFactory extends BaseDataLoaderFactory
{
    /**
     * @param non-empty-string $sourceDataDirPath Relative path inside "/data" dir where to read the data from.
     */
    public function createOutputDataLoader(
        ClientWrapper $clientWrapper,
        ComponentSpecification $component,
        Configuration $configuration,
        ?string $configId,
        ?string $configRowId,
        ?string $stagingWorkspaceId,
        string $sourceDataDirPath,
    ): OutputDataLoader {
        $stagingProvider = $this->createStagingProvider(
            StagingType::from($component->getOutputStagingStorage()),
            $stagingWorkspaceId,
        );

        $strategyFactory = new OutputStrategyFactory(
            $stagingProvider,
            $clientWrapper,
            $this->logger,
            FileFormat::from($component->getConfigurationFormat()),
        );

        return new OutputDataLoader(
            $strategyFactory,
            $clientWrapper,
            $component,
            $configuration,
            $configId,
            $configRowId,
            $this->logger,
            $sourceDataDirPath,
        );
    }
}
