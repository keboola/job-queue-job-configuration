<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping\DataLoader;

use Keboola\StagingProvider\Staging\File\LocalStaging;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStaging;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;
use LogicException;
use Psr\Log\LoggerInterface;

abstract class BaseDataLoaderFactory
{
    /**
     * @param string $dataDirectory "/data" for no-dind, something like "/tmp/run-abcd.123/data" in job-runner
     */
    public function __construct(
        protected readonly WorkspaceProvider $workspaceProvider,
        protected readonly LoggerInterface $logger,
        protected readonly string $dataDirectory,
    ) {
    }

    protected function createStagingProvider(
        StagingType $stagingType,
        ?string $stagingWorkspaceId,
    ): StagingProvider {
        $hasWorkspaceStaging = $stagingType->getStagingClass() === StagingClass::Workspace;
        $hasWorkspaceId = $stagingWorkspaceId !== null;
        if ($hasWorkspaceStaging !== $hasWorkspaceId) {
            throw new LogicException('Staging workspace ID must be configured for component with workspace staging.');
        }

        if ($stagingWorkspaceId === null) {
            $stagingWorkspace = null;
        } else {
            // we don't even need to "load" the workspace here as staging needs only its ID
            $stagingWorkspace = new WorkspaceStaging($stagingWorkspaceId);
        }

        return new StagingProvider(
            $stagingType,
            $stagingWorkspace,
            new LocalStaging($this->dataDirectory),
        );
    }
}
