<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Generator;
use Keboola\InputMapping\Staging\AbstractStagingDefinition;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Runtime;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StagingProvider\Provider\WorkspaceStagingProvider;
use Psr\Log\LoggerInterface;

class OutputDataLoader
{
    public function __construct(
        private readonly OutputStrategyFactory $outputStrategyFactory,
        private readonly LoggerInterface $logger,
        private readonly string $dataOutDir,
    ) {
    }

    public function storeOutput(
        ComponentSpecification $component,
        Configuration $jobConfiguration,
        ?string $branchId,
        ?string $runId,
        ?string $configId,
        ?string $configRowId,
        array $projectFeatures,
        bool $isFailedJob = false,
    ): ?LoadTableQueue {
        $this->logger->debug('Storing results.');

        $inputStorageConfig = $jobConfiguration->storage->input;
        $outputStorageConfig = $jobConfiguration->storage->output;

        $defaultBucketName = $outputStorageConfig->defaultBucket ?? '';
        if ($defaultBucketName === '') {
            $defaultBucketName = $this->getDefaultBucket($component, $configId);
        }

        $this->logger->debug('Uploading output tables and files.');

        $uploadTablesOptions = ['mapping' => $outputStorageConfig->tables->toArray()];

        $commonSystemMetadata = [
            AbstractWriter::SYSTEM_KEY_COMPONENT_ID => $component->getId(),
            AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID => $configId,
        ];
        if ($configRowId) {
            $commonSystemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID] = $configRowId;
        }
        $tableSystemMetadata = $fileSystemMetadata = $commonSystemMetadata;
        if ($branchId !== null) {
            $tableSystemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID] = $branchId;
        }

        $fileSystemMetadata[AbstractWriter::SYSTEM_KEY_RUN_ID] = $runId;

        // Get default bucket
        if ($defaultBucketName) {
            $uploadTablesOptions['bucket'] = $defaultBucketName;
            $this->logger->debug('Default bucket ' . $uploadTablesOptions['bucket']);
        }

        try {
            $fileWriter = new FileWriter($this->outputStrategyFactory);
            $fileWriter->setFormat($component->getConfigurationFormat());
            $fileWriter->uploadFiles(
                $this->dataOutDir . '/files/',
                ['mapping' => $outputStorageConfig->files->toArray()],
                $fileSystemMetadata,
                $component->getOutputStagingStorage(),
                [],
                $isFailedJob,
            );
            if ($this->useFileStorageOnly($component, $jobConfiguration->runtime)) {
                $fileWriter->uploadFiles(
                    $this->dataOutDir . '/tables/',
                    [],
                    $fileSystemMetadata,
                    $component->getOutputStagingStorage(),
                    $outputStorageConfig->tableFiles->toArray(),
                    $isFailedJob,
                );

                if (!$inputStorageConfig->files->isEmpty()) {
                    // tag input files
                    $fileWriter->tagFiles($inputStorageConfig->files->toArray());
                }

                return null;
            }
            $tableWriter = new TableWriter($this->outputStrategyFactory);
            $tableWriter->setFormat($component->getConfigurationFormat());
            $tableQueue = $tableWriter->uploadTables(
                $this->dataOutDir . '/tables/',
                $uploadTablesOptions,
                $tableSystemMetadata,
                $component->getOutputStagingStorage(),
                $isFailedJob,
                $this->getDataTypeSupport($component, $outputStorageConfig->dataTypeSupport),
            );

            if (!$inputStorageConfig->files->isEmpty()) {
                // tag input files
                $fileWriter->tagFiles($inputStorageConfig->files->toArray());
            }

            return $tableQueue;
        } catch (InvalidOutputException $ex) {
            throw new UserException($ex->getMessage(), previous: $ex);
        }
    }

    private function getDefaultBucket(ComponentSpecification $component, ?string $configId): string
    {
        if ($component->hasDefaultBucket()) {
            if (!$configId) {
                throw new UserException('Configuration ID not set, but is required for default_bucket option.');
            }
            return $component->getDefaultBucketName($configId);
        } else {
            return '';
        }
    }

    private function useFileStorageOnly(ComponentSpecification $component, ?Runtime $runtimeConfig): bool
    {
        return $component->allowUseFileStorageOnly() && $runtimeConfig?->useFileStorageOnly;
    }

    private function getDataTypeSupport(ComponentSpecification $component, ?string $dataTypeSupport): string
    {
        if (!$this->outputStrategyFactory->getClientWrapper()->getToken()->hasFeature('new-native-types')) {
            return 'none';
        }
        return $dataTypeSupport ?? $component->getDataTypesSupport();
    }

    public function getWorkspaceCredentials(): array
    {
        // this returns the first workspace found, which is ok so far because there can only be one
        // (ensured in validateStagingSetting()) working only with inputStrategyFactory, but
        // the workspace providers are shared between input and output, so it's "ok"
        foreach ($this->outputStrategyFactory->getStrategyMap() as $stagingDefinition) {
            foreach ($this->getStagingProviders($stagingDefinition) as $stagingProvider) {
                if (!$stagingProvider instanceof WorkspaceStagingProvider) {
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
