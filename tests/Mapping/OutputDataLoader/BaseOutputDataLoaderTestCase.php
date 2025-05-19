<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\Mapping\OutputDataLoader;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\BaseDataLoaderTestCase;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
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
        ?OutputStrategyFactory $outputStrategyFactory = null,
        LoggerInterface $logger = new NullLogger(),
        ?string $configId = null,
        ?bool $readOnlyWorkspace = null,
    ): OutputDataLoader {
        $clientWrapper = $clientWrapper ?? $this->clientWrapper;

        $outputStrategyFactory ??= $this->createOutputStrategyFactory(
            component: $component,
            clientWrapper: $clientWrapper,
            logger: $logger,
        );

        return new OutputDataLoader(
            clientWrapper: $clientWrapper,
            outputStrategyFactory: $outputStrategyFactory,
            logger: $logger,
            dataOutDir: '/data/out',
        );
    }
}
