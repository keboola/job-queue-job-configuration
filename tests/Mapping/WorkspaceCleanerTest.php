<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Psr\Log\NullLogger;

class WorkspaceCleanerTest extends BaseDataLoaderTestCase
{
    protected static function expectedDefaultTableBackend(): string
    {
        return 'snowflake';
    }

    public function testWorkspaceCleanupWhenInitialized(): void
    {
        $componentsApiClient = new Components($this->clientWrapper->getBasicClient());

        $componentId = 'keboola.runner-workspace-test';
        $component = new ComponentSpecification([
            'id' => $componentId,
            'data' => [
                'definition' => [
                    'type' => 'aws-ecr',
                    // phpcs:ignore Generic.Files.LineLength.MaxExceeded
                    'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-workspace-test',
                    'tag' => '1.6.2',
                ],
                'staging-storage' => [
                    'input' => 'workspace-snowflake',
                    'output' => 'workspace-snowflake',
                ],
            ],
        ]);

        $configId = $this->createConfig($componentId, 'test-workspaceCleaner-cleanupWhenInitialized');

        $workspaceListOptions = new ListConfigurationWorkspacesOptions();
        $workspaceListOptions->setComponentId($componentId)->setConfigurationId($configId);

        $logger = new NullLogger();

        // create InputStrategyFactory manually so we can trigger workspace creation
        $workspaceProviderFactory = $this->createWorkspaceProviderFactory(
            $component,
            $configId,
        );

        $inputStrategyFactory = $this->createInputStrategyFactory(
            $component,
            $workspaceProviderFactory,
        );

        // this causes workspace creation (so that we can test its cleanup)
        $inputStrategyFactory->getStrategyMap()['workspace-snowflake']->getTableDataProvider()->getCredentials();
        self::assertCount(1, $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions));

        $workspaceCleaner = $this->getWorkspaceCleaner(
            clientWrapper: $this->clientWrapper,
            configId: $configId,
            component: $component,
            workspaceProviderFactory: $workspaceProviderFactory,
            inputStrategyFactory: $inputStrategyFactory,
            logger: $logger,
        );

        $workspaceCleaner->cleanWorkspace(
            $component,
            $configId,
        );

        self::assertCount(0, $componentsApiClient->listConfigurationWorkspaces($workspaceListOptions));
    }

    /**
     * @param non-empty-string $configId
     * @return non-empty-string
     */
    private function createConfig(
        string $componentId,
        string $configId,
        array $configuration = [],
        ?StorageApiClient $storageApiClient = null,
    ): string {
        $storageApiClient ??= $this->clientWrapper->getBranchClient();
        $componentsApiClient = new Components($storageApiClient);

        try {
            $componentsApiClient->deleteConfiguration($componentId, $configId);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        $config = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configId)
            ->setName($configId)
            ->setConfiguration($configuration)
        ;

        $componentsApiClient->addConfiguration($config);

        return $configId;
    }
}
