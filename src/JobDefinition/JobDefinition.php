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
    ) {
        if (empty($this->configId) !== empty($this->configVersion)) {
            throw new InvalidArgumentException('configId and configVersion must either both be set or both left empty.');
        }
    }
}
