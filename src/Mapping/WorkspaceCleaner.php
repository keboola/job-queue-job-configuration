<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StagingProvider\Provider\NewWorkspaceProvider;
use Keboola\StorageApi\ClientException;
use Psr\Log\LoggerInterface;

class WorkspaceCleaner
{
    use StagingProviderAwareTrait;

    public function __construct(
        private readonly OutputStrategyFactory $outputStrategyFactory,
        private readonly InputStrategyFactory $inputStrategyFactory,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function cleanWorkspace(): void
    {
        $cleanedProviders = [];
        $maps = array_merge(
            $this->inputStrategyFactory->getStrategyMap(),
            $this->outputStrategyFactory->getStrategyMap(),
        );
        foreach ($maps as $stagingDefinition) {
            foreach ($this->getStagingProviders($stagingDefinition) as $stagingProvider) {
                if (!$stagingProvider instanceof NewWorkspaceProvider) {
                    continue;
                }
                if (in_array($stagingProvider, $cleanedProviders, true)) {
                    continue;
                }

                try {
                    $stagingProvider->cleanup();
                    $cleanedProviders[] = $stagingProvider;
                } catch (ClientException $e) {
                    if ($e->getCode() === 404) {
                        // workspace is already deleted
                        continue;
                    }

                    // ignore errors if the cleanup fails because we a) can't fix it b) should not break the job
                    $this->logger->error('Failed to cleanup workspace: ' . $e->getMessage());
                }
            }
        }
    }
}
