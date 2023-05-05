<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Backend;
use Keboola\StagingProvider\Staging\Workspace\AbsWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\RedshiftWorkspaceStaging;
use Keboola\StagingProvider\WorkspaceProviderFactory\AbstractCachedWorkspaceProviderFactory;
use Keboola\StagingProvider\WorkspaceProviderFactory\ComponentWorkspaceProviderFactory;
use Keboola\StagingProvider\WorkspaceProviderFactory\Configuration\WorkspaceBackendConfig;
use Keboola\StagingProvider\WorkspaceProviderFactory\ExistingDatabaseWorkspaceProviderFactory;
use Keboola\StagingProvider\WorkspaceProviderFactory\ExistingFilesystemWorkspaceProviderFactory;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Psr\Log\LoggerInterface;

class WorkspaceProviderFactoryFactory
{
    public function __construct(
        private readonly Components $componentsApiClient,
        private readonly Workspaces $workspacesApiClient,
        private readonly LoggerInterface $logger
    ) {
    }

    /**
     * @param non-empty-string $stagingStorage
     * @param null|non-empty-string $configId
     */
    public function getWorkspaceProviderFactory(
        string $stagingStorage,
        ComponentSpecification $component,
        ?string $configId,
        ?Backend $backendConfig,
        ?bool $useReadonlyRole
    ): AbstractCachedWorkspaceProviderFactory {
        /* There can only be one workspace type (ensured in validateStagingSetting()) - so we're checking
            just input staging here (because if it is workspace, it must be the same as output mapping). */
        if ($configId && ($stagingStorage === AbstractStrategyFactory::WORKSPACE_ABS)) {
            // ABS workspaces are persistent, but only if configId is present
            $workspaceProviderFactory = $this->getWorkspaceFactoryForPersistentAbsWorkspace($component, $configId);
        } elseif ($configId && ($stagingStorage === AbstractStrategyFactory::WORKSPACE_REDSHIFT)) {
            // Redshift workspaces are persistent, but only if configId is present
            $workspaceProviderFactory = $this->getWorkspaceFactoryForPersistentRedshiftWorkspace($component, $configId);
        } else {
            $workspaceProviderFactory = new ComponentWorkspaceProviderFactory(
                $this->componentsApiClient,
                $this->workspacesApiClient,
                $component->getId(),
                $configId,
                new WorkspaceBackendConfig($backendConfig?->type),
                $useReadonlyRole
            );
            $this->logger->notice(sprintf(
                'Created a new %s workspace.',
                $useReadonlyRole ? 'readonly ephemeral' : 'ephemeral'
            ));
        }
        return $workspaceProviderFactory;
    }

    /**
     * @param non-empty-string $configId
     */
    private function getWorkspaceFactoryForPersistentRedshiftWorkspace(
        ComponentSpecification $component,
        string $configId
    ): ExistingDatabaseWorkspaceProviderFactory {
        $listOptions = (new ListConfigurationWorkspacesOptions())
            ->setComponentId($component->getId())
            ->setConfigurationId($configId);
        $workspaces = $this->componentsApiClient->listConfigurationWorkspaces($listOptions);

        if (count($workspaces) === 0) {
            $workspace = $this->componentsApiClient->createConfigurationWorkspace(
                $component->getId(),
                $configId,
                ['backend' => RedshiftWorkspaceStaging::getType()],
                true
            );
            $workspaceId = (string) $workspace['id'];
            $password = $workspace['connection']['password'];
            $this->logger->info(sprintf('Created a new persistent workspace "%s".', $workspaceId));
        } elseif (count($workspaces) === 1) {
            $workspaceId = (string) $workspaces[0]['id'];
            $password = $this->workspacesApiClient->resetWorkspacePassword($workspaceId)['password'];
            $this->logger->info(sprintf('Reusing persistent workspace "%s".', $workspaceId));
        } else {
            $ids = array_column($workspaces, 'id');
            sort($ids, SORT_NUMERIC);
            $workspaceId = (string) $ids[0];
            $this->logger->warning(sprintf(
                'Multiple workspaces (total %s) found (IDs: %s) for configuration "%s" of component "%s", using "%s".',
                count($workspaces),
                implode(',', $ids),
                $configId,
                $component->getId(),
                $workspaceId
            ));
            $password = $this->workspacesApiClient->resetWorkspacePassword($workspaceId)['password'];
        }
        return new ExistingDatabaseWorkspaceProviderFactory(
            $this->workspacesApiClient,
            $workspaceId,
            $password
        );
    }

    /**
     * @param non-empty-string $configId
     */
    private function getWorkspaceFactoryForPersistentAbsWorkspace(
        ComponentSpecification $component,
        string $configId,
    ): ExistingFilesystemWorkspaceProviderFactory {
        // ABS workspaces are persistent, but only if configId is present
        $listOptions = (new ListConfigurationWorkspacesOptions())
            ->setComponentId($component->getId())
            ->setConfigurationId($configId);
        $workspaces = $this->componentsApiClient->listConfigurationWorkspaces($listOptions);

        if (count($workspaces) === 0) {
            $workspace = $this->componentsApiClient->createConfigurationWorkspace(
                $component->getId(),
                $configId,
                ['backend' => AbsWorkspaceStaging::getType()],
                true
            );
            $workspaceId = (string) $workspace['id'];
            $connectionString = $workspace['connection']['connectionString'];
            $this->logger->info(sprintf('Created a new persistent workspace "%s".', $workspaceId));
        } elseif (count($workspaces) === 1) {
            $workspaceId = (string) $workspaces[0]['id'];
            $connectionString = $this->workspacesApiClient->resetWorkspacePassword($workspaceId)['connectionString'];
            $this->logger->info(sprintf('Reusing persistent workspace "%s".', $workspaceId));
        } else {
            throw new ApplicationException(sprintf(
                'Multiple workspaces (total %s) found (IDs: %s, %s) for configuration "%s" of component "%s".',
                count($workspaces),
                $workspaces[0]['id'],
                $workspaces[1]['id'],
                $configId,
                $component->getId()
            ));
        }
        return new ExistingFilesystemWorkspaceProviderFactory(
            $this->workspacesApiClient,
            $workspaceId,
            $connectionString
        );
    }
}
