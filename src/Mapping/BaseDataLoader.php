<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;

abstract class BaseDataLoader
{
    protected function validateComponentStagingSetting(ComponentSpecification $component): void
    {
        $workspaceTypes = [
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            AbstractStrategyFactory::WORKSPACE_BIGQUERY,
        ];
        if (in_array($component->getInputStagingStorage(), $workspaceTypes) &&
            in_array($component->getOutputStagingStorage(), $workspaceTypes) &&
            $component->getInputStagingStorage() !== $component->getOutputStagingStorage()
        ) {
            throw new ApplicationException(sprintf(
                'Component staging setting mismatch - input: "%s", output: "%s".',
                $component->getInputStagingStorage(),
                $component->getOutputStagingStorage(),
            ));
        }
    }
}
