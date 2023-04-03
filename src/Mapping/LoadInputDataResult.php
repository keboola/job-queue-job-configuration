<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Table\Result as InputTableResult;

readonly class LoadInputDataResult
{
    public function __construct(
        public InputTableResult $inputTableResult,
        public InputFileStateList $inputFileStateList,
    ) {
    }

    public function toArray(): array
    {
        return [
            'tables' => [
                'tables' => $this->inputTableResult->getTables(),
                'metrics' => $this->inputTableResult->getMetrics(),
                'inputTableStateList' => $this->inputTableResult->getInputTableStateList(),
            ],
            'inputFileStateList' => $this->inputFileStateList->jsonSerialize(),
        ];
    }
}
