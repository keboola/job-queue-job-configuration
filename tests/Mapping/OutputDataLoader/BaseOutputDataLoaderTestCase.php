<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\Mapping\DataLoader\OutputDataLoader;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\BaseDataLoaderTestCase;
use Keboola\StorageApiBranch\ClientWrapper;
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
        return OutputDataLoader::create(
            new NullLogger(),
            $clientWrapper ?? $this->clientWrapper,
            $component,
            $config,
            $configId,
            $configRowId,
            stagingWorkspaceId: null, // TODO
            dataDirPath: $this->getDataDirPath(),
            sourceDataDirSubpath: 'out/',
        );
    }
}
