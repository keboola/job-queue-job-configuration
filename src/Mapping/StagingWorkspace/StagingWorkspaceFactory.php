<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping\StagingWorkspace;

use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Workspace\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Workspace\Configuration\NewWorkspaceConfig;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApiBranch\StorageApiToken;
use Psr\Log\LoggerInterface;

class StagingWorkspaceFactory
{
    public function __construct(
        private readonly WorkspaceProvider $workspaceProvider,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function createStagingWorkspaceFacade(
        StorageApiToken $storageApiToken,
        ComponentSpecification $component,
        Configuration $configuration,
        ?string $configId,
    ): ?StagingWorkspaceFacade {
        $stagingType = $this->resolveStagingForComponent($component);
        if ($stagingType->getStagingClass() !== StagingClass::Workspace) {
            return null;
        }

        $externallyManagedWorkspaceCredentials = $configuration->runtime?->backend?->workspaceCredentials;
        if ($externallyManagedWorkspaceCredentials) {
            return $this->getExistingWorkspaceStagingFacade(
                $externallyManagedWorkspaceCredentials,
            );
        } else {
            return $this->getNewWorkspaceStagingFacade(
                $stagingType,
                $component,
                $configuration,
                $configId,
                $storageApiToken,
            );
        }
    }

    private function resolveStagingForComponent(ComponentSpecification $component): StagingType
    {
        $inputStagingType = StagingType::from($component->getInputStagingStorage());
        $outputStagingType = StagingType::from($component->getOutputStagingStorage());

        if ($inputStagingType->getStagingClass() === StagingClass::Workspace &&
            $outputStagingType->getStagingClass() === StagingClass::Workspace &&
            $inputStagingType !== $outputStagingType
        ) {
            throw new ApplicationException(sprintf(
                'Component staging setting mismatch - input: "%s", output: "%s".',
                $inputStagingType->value,
                $outputStagingType->value,
            ));
        }

        // now we're sure that input and output staging type are the same for workspace staging
        return $inputStagingType;
    }

    private function getExistingWorkspaceStagingFacade(
        WorkspaceCredentials $externallyManagedWorkspaceCredentials,
    ): StagingWorkspaceFacade {
        $this->logger->notice(sprintf(
            'Using provided workspace "%s".',
            $externallyManagedWorkspaceCredentials->id,
        ));

        $workspace = $this->workspaceProvider->getExistingWorkspace(
            $externallyManagedWorkspaceCredentials->id,
            $externallyManagedWorkspaceCredentials->getCredentials(),
        );

        return new StagingWorkspaceFacade(
            $this->workspaceProvider,
            $this->logger,
            $workspace,
            isReusable: true, // Externally managed workspaces are persistent
        );
    }

    private function getNewWorkspaceStagingFacade(
        StagingType $stagingType,
        ComponentSpecification $component,
        Configuration $configuration,
        ?string $configId,
        StorageApiToken $storageApiToken,
    ): StagingWorkspaceFacade {
        $backendConfig = $configuration->runtime?->backend;
        $useReadonlyRole = $configuration->storage->input->readOnlyStorageAccess;

        $this->logger->notice(sprintf(
            'Creating a new %s workspace.',
            $useReadonlyRole ? 'readonly ephemeral' : 'ephemeral',
        ));

        $workspaceLoginType = null;
        if ($stagingType === StagingType::WorkspaceSnowflake && $component->useSnowflakeKeyPairAuth()) {
            $workspaceLoginType = WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR;
        }

        $workspaceConfig = new NewWorkspaceConfig(
            stagingType: $stagingType,
            componentId: $component->getId(),
            configId: $configId,
            size: $backendConfig?->type,
            useReadonlyRole: $useReadonlyRole,
            networkPolicy: NetworkPolicy::SYSTEM,
            loginType: $workspaceLoginType,
        );

        $workspace = $this->workspaceProvider->createNewWorkspace(
            $storageApiToken,
            $workspaceConfig,
        );

        return new StagingWorkspaceFacade(
            $this->workspaceProvider,
            $this->logger,
            $workspace,
            isReusable: false,
        );
    }
}
