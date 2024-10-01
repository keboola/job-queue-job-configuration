<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition;

readonly class JobDefinition
{
    public function __construct(
        public Component\ComponentSpecification $component,
        public ?string $configId,
        public ?string $rowId,
        public bool $isDisabled,
        public Configuration\Configuration $configuration,
        public State\State $state,
        /** @var 'dev'|'default' same as ObjectEncryptor::BRANCH_TYPE_*, but we don't depend on object encryptor */
        public string $branchType = 'default',
    ) {
    }
}
