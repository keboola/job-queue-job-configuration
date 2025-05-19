<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Table\Result as InputTableResult;
use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class InputDataLoader extends BaseDataLoader
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly InputStrategyFactory $inputStrategyFactory,
        private readonly LoggerInterface $logger,
        private readonly string $dataInDir,
    ) {
    }

    public function loadInputData(
        ComponentSpecification $component,
        Configuration $jobConfiguration,
        State $jobState,
    ): LoadInputDataResult {
        $this->validateComponentStagingSetting($component);

        $inputTableResult = new InputTableResult();
        $inputTableResult->setInputTableStateList(new InputTableStateList([]));

        $inputFileStateList = new InputFileStateList([]);

        $inputConfig = $jobConfiguration->storage->input;
        $inputState = $jobState->storage->input;

        $reader = new Reader(
            $this->clientWrapper,
            $this->logger,
            $this->inputStrategyFactory,
        );
        try {
            if (!$inputConfig->tables->isEmpty()) {
                $this->logger->debug('Downloading source tables.');

                $readerOptions = new ReaderOptions(
                    !$component->allowBranchMapping(),
                    preserveWorkspace: false,
                );

                $inputTableResult = $reader->downloadTables(
                    new InputTableOptionsList($inputConfig->tables->toArray()),
                    new InputTableStateList($inputState->tables->toArray()),
                    $this->dataInDir . '/tables/',
                    $readerOptions,
                );
            }

            if (!$inputConfig->files->isEmpty()) {
                $this->logger->debug('Downloading source files.');
                $inputFileStateList = $reader->downloadFiles(
                    $inputConfig->files->toArray(),
                    $this->dataInDir . '/files/',
                    new InputFileStateList($inputState->files->toArray()),
                );
            }
        } catch (ClientException $e) {
            throw new UserException('Cannot import data from Storage API: ' . $e->getMessage(), previous: $e);
        } catch (InvalidInputException $e) {
            throw new UserException($e->getMessage(), previous: $e);
        }

        return new LoadInputDataResult($inputTableResult, $inputFileStateList);
    }
}
