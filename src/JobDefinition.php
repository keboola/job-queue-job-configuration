<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration;

readonly class JobDefinition
{
    public function __construct(
        public Component $component,
        public ?string $configId,
        public ?string $rowId,
        public Configuration\Configuration $configuration,
        public State\State $state,
    ) {
    }
}
