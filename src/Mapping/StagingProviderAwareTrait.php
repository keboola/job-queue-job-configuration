<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Generator;
use Keboola\InputMapping\Staging\AbstractStagingDefinition;
use Keboola\InputMapping\Staging\ProviderInterface;

trait StagingProviderAwareTrait
{
    /** @return Generator<ProviderInterface|null> */
    private function getStagingProviders(AbstractStagingDefinition $stagingDefinition): Generator
    {
        yield $stagingDefinition->getFileDataProvider();
        yield $stagingDefinition->getFileMetadataProvider();
        yield $stagingDefinition->getTableDataProvider();
        yield $stagingDefinition->getTableMetadataProvider();
    }
}
