<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StagingProvider\Provider\NewWorkspaceStagingProvider;
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

    public function cleanWorkspace(ComponentSpecification $component, ?string $configId): void
    {
        $cleanedProviders = [];
        $maps = array_merge(
            $this->inputStrategyFactory->getStrategyMap(),
            $this->outputStrategyFactory->getStrategyMap(),
        );
        foreach ($maps as $stagingDefinition) {
            foreach ($this->getStagingProviders($stagingDefinition) as $stagingProvider) {
                if (!$stagingProvider instanceof NewWorkspaceStagingProvider) {
                    continue;
                }
                if (in_array($stagingProvider, $cleanedProviders, true)) {
                    continue;
                }
                /* don't clean ABS workspaces or Redshift workspaces which are reusable if created for a config.

                    The whole condition and the isReusableWorkspace method can probably be completely removed,
                    because now it is distinguished between NewWorkspaceStagingProvider (cleanup) and
                    ExistingWorkspaceStagingProvider (no cleanup).

                    However, since ABS and Redshift workspaces are not used in real life and badly tested, I don't
                    want to remove it now.
                 */
                if ($configId && $this->isReusableWorkspace($component)) {
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

    private function isReusableWorkspace(ComponentSpecification $component): bool
    {
        return
            $component->getInputStagingStorage() === AbstractStrategyFactory::WORKSPACE_ABS ||
            $component->getInputStagingStorage() === AbstractStrategyFactory::WORKSPACE_REDSHIFT ||
            $component->getOutputStagingStorage() === AbstractStrategyFactory::WORKSPACE_ABS ||
            $component->getOutputStagingStorage() === AbstractStrategyFactory::WORKSPACE_REDSHIFT
        ;
    }
}
