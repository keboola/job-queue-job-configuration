<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition;

use InvalidArgumentException;

readonly class JobDefinition
{
    public function __construct(
        public Component\ComponentSpecification $component,
        public ?string $configId,
        public ?string $configVersion,
        public ?string $rowId,
        public bool $isDisabled,
        public Configuration\Configuration $configuration,
        public State\State $state,
        public array $replacedVariablesValues,
    ) {
        if (!in_array($this->configVersion, [null, ''], true) && in_array($this->configId, [null, ''], true)) {
            throw new InvalidArgumentException('configVersion cannot be set if configId is empty.');
        }
    }
}
