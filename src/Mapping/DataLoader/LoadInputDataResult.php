<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping\DataLoader;

use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Result as InputTableResult;

readonly class LoadInputDataResult
{
    public function __construct(
        public InputTableResult $inputTableResult,
        public InputFileStateList $inputFileStateList,
    ) {
    }

    public static function createEmpty(): self
    {
        $inputTableResult = new InputTableResult();
        // the state list is not initialized by default and accessing it would fail
        $inputTableResult->setInputTableStateList(new InputTableStateList([]));

        return new self(
            $inputTableResult,
            new InputFileStateList([]),
        );
    }
}
