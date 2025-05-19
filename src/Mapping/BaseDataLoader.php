<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingType;

abstract class BaseDataLoader
{
    protected function getComponentInputStagingType(ComponentSpecification $componentSpecification): StagingType
    {
        return StagingType::from($componentSpecification->getInputStagingStorage());
    }

    protected function getComponentOutputStagingType(ComponentSpecification $componentSpecification): StagingType
    {
        return StagingType::from($componentSpecification->getOutputStagingStorage());
    }

    protected function validateComponentStagingSetting(ComponentSpecification $component): void
    {
        $input = $this->getComponentInputStagingType($component);
        $output = $this->getComponentOutputStagingType($component);

        if ($input->getStagingClass() === StagingClass::Workspace &&
            $output->getStagingClass() === StagingClass::Workspace &&
            $input !== $output
        ) {
            throw new ApplicationException(sprintf(
                'Component staging setting mismatch - input: "%s", output: "%s".',
                $input->value,
                $output->value,
            ));
        }
    }
}
