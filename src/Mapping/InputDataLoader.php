<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Generator;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStagingDefinition;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\ProviderInterface;
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
use Keboola\StagingProvider\Provider\ExistingWorkspaceStagingProvider;
use Keboola\StagingProvider\Provider\NewWorkspaceStagingProvider;
use Keboola\StorageApi\ClientException;
use Psr\Log\LoggerInterface;

class InputDataLoader extends BaseDataLoader
{
    public function __construct(
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

        $reader = new Reader($this->inputStrategyFactory);
        try {
            if (!$inputConfig->tables->isEmpty()) {
                $this->logger->debug('Downloading source tables.');

                /*
                 * preserve is true only for ABS Workspaces which are persistent (shared across runs) (preserve = true).
                 * Redshift workspaces are reusable, but still cleaned up before each run (preserve = false). Other
                 * workspaces (Snowflake, Local) are ephemeral (thus the preserver flag is irrelevant for them).
                 */
                $preserveWorkspace = $component->getInputStagingStorage() === AbstractStrategyFactory::WORKSPACE_ABS;

                $readerOptions = new ReaderOptions(
                    !$component->allowBranchMapping(),
                    $preserveWorkspace,
                );

                $inputTableResult = $reader->downloadTables(
                    new InputTableOptionsList($inputConfig->tables->toArray()),
                    new InputTableStateList($inputState->tables->toArray()),
                    $this->dataInDir . '/tables/',
                    $component->getInputStagingStorage(),
                    $readerOptions,
                );
            }

            if (!$inputConfig->files->isEmpty()) {
                $this->logger->debug('Downloading source files.');
                $inputFileStateList = $reader->downloadFiles(
                    $inputConfig->files->toArray(),
                    $this->dataInDir . '/files/',
                    $component->getInputStagingStorage(),
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

    public function getWorkspaceCredentials(): array
    {
        // this returns the first workspace found, which is ok so far because there can only be one
        // (ensured in validateStagingSetting()) working only with inputStrategyFactory, but
        // the workspace providers are shared between input and output, so it's "ok"
        foreach ($this->inputStrategyFactory->getStrategyMap() as $stagingDefinition) {
            foreach ($this->getStagingProviders($stagingDefinition) as $stagingProvider) {
                if (!$stagingProvider instanceof NewWorkspaceStagingProvider &&
                    !$stagingProvider instanceof ExistingWorkspaceStagingProvider) {
                    continue;
                }

                return $stagingProvider->getCredentials();
            }
        }
        return [];
    }

    /** @return Generator<ProviderInterface|null> */
    private function getStagingProviders(AbstractStagingDefinition $stagingDefinition): Generator
    {
        yield $stagingDefinition->getFileDataProvider();
        yield $stagingDefinition->getFileMetadataProvider();
        yield $stagingDefinition->getTableDataProvider();
        yield $stagingDefinition->getTableMetadataProvider();
    }
}
