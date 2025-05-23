<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping\DataLoader;

use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\DataTypeSupport;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Runtime;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Output;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\TableLoader;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class OutputDataLoader
{
    private readonly string $sourceDataDirPath;

    /**
     * @param non-empty-string $sourceDataDirPath Relative path inside "/data" dir where to read the data from.
     */
    public function __construct(
        private readonly OutputStrategyFactory $outputStrategyFactory,
        private readonly ClientWrapper $clientWrapper,
        private readonly ComponentSpecification $component,
        private readonly Configuration $configuration,
        private readonly ?string $configId,
        private readonly ?string $configRowId,
        private readonly LoggerInterface $logger,
        string $sourceDataDirPath,
    ) {
        $this->sourceDataDirPath = '/' . trim($sourceDataDirPath, '/'); // normalize to a path with leading slash
    }

    public function storeOutput(
        bool $isFailedJob = false,
    ): ?LoadTableQueue {
        $this->logger->debug('Storing results.');

        $inputStorageConfig = $this->configuration->storage->input;
        $outputStorageConfig = $this->configuration->storage->output;
        $clientWrapper = $this->clientWrapper;

        $defaultBucketName = $outputStorageConfig->defaultBucket ?? '';
        if ($defaultBucketName === '') {
            $defaultBucketName = $this->getDefaultBucket($this->component, $this->configId);
        }

        $this->logger->debug('Uploading output tables and files.');

        $uploadTablesOptions = ['mapping' => $outputStorageConfig->tables->toArray()];

        $commonSystemMetadata = [
            SystemMetadata::SYSTEM_KEY_COMPONENT_ID => $this->component->getId(),
            SystemMetadata::SYSTEM_KEY_CONFIGURATION_ID => $this->configId,
        ];
        if ($this->configRowId) {
            $commonSystemMetadata[SystemMetadata::SYSTEM_KEY_CONFIGURATION_ROW_ID] = $this->configRowId;
        }
        $tableSystemMetadata = $fileSystemMetadata = $commonSystemMetadata;
        if ($clientWrapper->isDevelopmentBranch()) {
            $tableSystemMetadata[SystemMetadata::SYSTEM_KEY_BRANCH_ID] = $clientWrapper->getBranchId();
        }

        $fileSystemMetadata[SystemMetadata::SYSTEM_KEY_RUN_ID] = $clientWrapper->getBranchClient()->getRunId();

        // Get default bucket
        if ($defaultBucketName) {
            $uploadTablesOptions['bucket'] = $defaultBucketName;
            $this->logger->debug('Default bucket ' . $uploadTablesOptions['bucket']);
        }

        $treatValuesAsNull = $this->configuration->storage->output->treatValuesAsNull;
        if ($treatValuesAsNull !== null) {
            $uploadTablesOptions['treat_values_as_null'] = $treatValuesAsNull;
        }

        try {
            $fileWriter = new FileWriter(
                $clientWrapper,
                $this->logger,
                $this->outputStrategyFactory,
            );
            $fileWriter->uploadFiles(
                $this->sourceDataDirPath . '/files/',
                ['mapping' => $outputStorageConfig->files->toArray()],
                $fileSystemMetadata,
                [],
                $isFailedJob,
            );
            if ($this->useFileStorageOnly($this->component, $this->configuration->runtime)) {
                $fileWriter->uploadFiles(
                    $this->sourceDataDirPath . '/tables/',
                    [],
                    $fileSystemMetadata,
                    $outputStorageConfig->tableFiles->toArray(),
                    $isFailedJob,
                );

                if (!$inputStorageConfig->files->isEmpty()) {
                    // tag input files
                    $fileWriter->tagFiles($inputStorageConfig->files->toArray());
                }

                return null;
            }

            $tableLoader = new TableLoader(
                logger: $this->logger,
                clientWrapper: $clientWrapper,
                strategyFactory: $this->outputStrategyFactory,
            );

            $mappingSettings = new OutputMappingSettings(
                configuration: $uploadTablesOptions,
                sourcePathPrefix: $this->sourceDataDirPath . '/tables/',
                storageApiToken: $clientWrapper->getToken(),
                isFailedJob: $isFailedJob,
                dataTypeSupport: $this->getDataTypeSupport($this->component, $outputStorageConfig)->value,
            );

            $tableQueue = $tableLoader->uploadTables(
                configuration: $mappingSettings,
                systemMetadata: new SystemMetadata($tableSystemMetadata),
            );

            if (!$inputStorageConfig->files->isEmpty() && !$isFailedJob) {
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

    private function getDataTypeSupport(ComponentSpecification $component, Output $outputStorageConfig): DataTypeSupport
    {
        if (!$this->clientWrapper->getToken()->hasFeature('new-native-types')) {
            return DataTypeSupport::NONE;
        }
        return $outputStorageConfig->dataTypeSupport ?? $component->getDataTypesSupport();
    }
}
