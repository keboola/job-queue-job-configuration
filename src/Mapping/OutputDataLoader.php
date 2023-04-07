<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Component;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Runtime;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\OutputMapping\Writer\TableWriter;
use Psr\Log\LoggerInterface;

class OutputDataLoader
{
    private const TYPED_TABLES_FEATURE = 'tables-definition';
    private const NATIVE_TYPES_FEATURE = 'native-types';

    public function __construct(
        private readonly OutputStrategyFactory $outputStrategyFactory,
        private readonly LoggerInterface $logger,
        private readonly string $dataOutDir,
    ) {
    }

    public function storeOutput(
        Component $component,
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

        // Check whether we are creating typed tables
        $createTypedTables = in_array(self::TYPED_TABLES_FEATURE, $projectFeatures, true)
            && in_array(self::NATIVE_TYPES_FEATURE, $projectFeatures, true);

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
                $createTypedTables,
                $isFailedJob,
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

    private function getDefaultBucket(Component $component, ?string $configId): string
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

    private function useFileStorageOnly(Component $component, ?Runtime $runtimeConfig): bool
    {
        return $component->allowUseFileStorageOnly() && $runtimeConfig?->useFileStorageOnly;
    }
}
