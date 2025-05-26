<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping\DataLoader;

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
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class InputDataLoader
{
    private readonly string $targetDataDirPath;

    /**
     * @param non-empty-string $targetDataDirPath Relative path inside "/data" dir where to put the loaded data to.
     */
    public function __construct(
        private readonly InputStrategyFactory $inputStrategyFactory,
        private readonly ClientWrapper $clientWrapper,
        private readonly ComponentSpecification $component,
        private readonly Configuration $jobConfiguration,
        private readonly State $jobState,
        private readonly LoggerInterface $logger,
        string $targetDataDirPath,
    ) {
        $this->targetDataDirPath = '/' . trim($targetDataDirPath, '/'); // normalize to a path with leading slash
    }

    /**
     * @param non-empty-string $dataDirPath "/data" for no-dind, something like "/tmp/run-abcd.123/data" in job-runner.
     * @param non-empty-string $targetDataDirSubpath Relative path inside $dataDir dir where to put the loaded data to.
     */
    public static function create(
        LoggerInterface $logger,
        ClientWrapper $clientWrapper,
        ComponentSpecification $component,
        Configuration $jobConfiguration,
        State $jobState,
        ?string $stagingWorkspaceId,
        string $dataDirPath,
        string $targetDataDirSubpath,
    ): self {
        $stagingProvider = new StagingProvider(
            StagingType::from($component->getInputStagingStorage()),
            $dataDirPath,
            $stagingWorkspaceId,
        );

        $strategyFactory = new InputStrategyFactory(
            $stagingProvider,
            $clientWrapper,
            $logger,
            FileFormat::from($component->getConfigurationFormat()),
        );

        return new self(
            $strategyFactory,
            $clientWrapper,
            $component,
            $jobConfiguration,
            $jobState,
            $logger,
            $targetDataDirSubpath,
        );
    }

    public function loadInputData(): LoadInputDataResult
    {
        $inputTableResult = new InputTableResult();
        $inputTableResult->setInputTableStateList(new InputTableStateList([]));

        $inputFileStateList = new InputFileStateList([]);

        $inputConfig = $this->jobConfiguration->storage->input;
        $inputState = $this->jobState->storage->input;

        $reader = new Reader(
            $this->clientWrapper,
            $this->logger,
            $this->inputStrategyFactory,
        );
        try {
            if (!$inputConfig->tables->isEmpty()) {
                $this->logger->debug('Downloading source tables.');

                $readerOptions = new ReaderOptions(
                    !$this->component->allowBranchMapping(),
                    preserveWorkspace: false,
                );

                $inputTableResult = $reader->downloadTables(
                    new InputTableOptionsList($inputConfig->tables->toArray()),
                    new InputTableStateList($inputState->tables->toArray()),
                    $this->targetDataDirPath . '/tables/',
                    $readerOptions,
                );
            }

            if (!$inputConfig->files->isEmpty()) {
                $this->logger->debug('Downloading source files.');
                $inputFileStateList = $reader->downloadFiles(
                    $inputConfig->files->toArray(),
                    $this->targetDataDirPath . '/files/',
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
