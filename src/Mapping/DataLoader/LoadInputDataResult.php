<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping\DataLoader;

use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Table\Result as InputTableResult;

readonly class LoadInputDataResult
{
    public function __construct(
        public InputTableResult $inputTableResult,
        public InputFileStateList $inputFileStateList,
    ) {
    }
}
