<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State;

readonly class JobDefinition
{
    public function __construct(
        public Component\Component $component,
        public ?string $configId,
        public ?string $rowId,
        public bool $isDisabled,
        public Configuration\Configuration $configuration,
        public State\State $state,
    ) {
    }
}
