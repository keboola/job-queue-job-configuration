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
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class OutputDataLoader
{
    private readonly string $sourceDataDirPath;

    /**
     * @param non-empty-string $sourceDataDirPath Relative path inside "/data" dir where to read the data from.
     */
    public function __construct(
        private readonly FileWriter $fileWriter,
        private readonly TableLoader $tableLoader,
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

    /**
     * @param non-empty-string $dataDirPath "/data" for no-dind, something like "/tmp/run-abcd.123/data" in job-runner.
     * @param non-empty-string $sourceDataDirSubpath Relative path inside $dataDir dir where to read the data from.
     */
    public static function create(
        LoggerInterface $logger,
        ClientWrapper $clientWrapper,
        ComponentSpecification $component,
        Configuration $configuration,
        ?string $configId,
        ?string $configRowId,
        ?string $stagingWorkspaceId,
        string $dataDirPath,
        string $sourceDataDirSubpath,
    ): ?self {
        $componentOutputStagingType = StagingType::from($component->getOutputStagingStorage());
        if ($componentOutputStagingType->getStagingClass() === StagingClass::None) {
            return null;
        }

        $stagingProvider = new StagingProvider(
            $componentOutputStagingType,
            $dataDirPath,
            $stagingWorkspaceId,
        );

        $strategyFactory = new OutputStrategyFactory(
            $stagingProvider,
            $clientWrapper,
            $logger,
            FileFormat::from($component->getConfigurationFormat()),
        );

        $fileWriter = new FileWriter(
            $clientWrapper,
            $logger,
            $strategyFactory,
        );

        $tableLoader = new TableLoader(
            logger: $logger,
            clientWrapper: $clientWrapper,
            strategyFactory: $strategyFactory,
        );

        return new self(
            $fileWriter,
            $tableLoader,
            $clientWrapper,
            $component,
            $configuration,
            $configId,
            $configRowId,
            $logger,
            $sourceDataDirSubpath,
        );
    }

    public function storeOutput(
        bool $isFailedJob = false,
    ): ?LoadTableQueue {
        $this->logger->debug('Storing results.');

        $inputStorageConfig = $this->configuration->storage->input;
        $outputStorageConfig = $this->configuration->storage->output;

        $this->logger->debug('Uploading output tables and files.');

        try {
            $fileSystemMetadata = $this->buildFilesystemMetadata();
            $this->fileWriter->uploadFiles(
                $this->sourceDataDirPath . '/files/',
                ['mapping' => $outputStorageConfig->files->toArray()],
                $fileSystemMetadata,
                [],
                $isFailedJob,
            );

            if ($this->useFileStorageOnly($this->component, $this->configuration->runtime)) {
                $this->fileWriter->uploadFiles(
                    $this->sourceDataDirPath . '/tables/',
                    [],
                    $fileSystemMetadata,
                    $outputStorageConfig->tableFiles->toArray(),
                    $isFailedJob,
                );

                if (!$inputStorageConfig->files->isEmpty()) {
                    // tag input files
                    $this->fileWriter->tagFiles($inputStorageConfig->files->toArray());
                }

                return null;
            }

            $mappingSettings = new OutputMappingSettings(
                configuration: $this->buildUploadTableOptions($outputStorageConfig),
                sourcePathPrefix: $this->sourceDataDirPath . '/tables/',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: $isFailedJob,
                dataTypeSupport: $this->getDataTypeSupport($this->component, $outputStorageConfig)->value,
            );

            $tableQueue = $this->tableLoader->uploadTables(
                configuration: $mappingSettings,
                systemMetadata: new SystemMetadata($this->buildTableMetadata()),
            );

            if (!$inputStorageConfig->files->isEmpty() && !$isFailedJob) {
                // tag input files
                $this->fileWriter->tagFiles($inputStorageConfig->files->toArray());
            }

            return $tableQueue;
        } catch (InvalidOutputException $ex) {
            throw new UserException($ex->getMessage(), previous: $ex);
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

    private function buildCommonMetadata(): array
    {
        $metadata = [
            SystemMetadata::SYSTEM_KEY_COMPONENT_ID => $this->component->getId(),
            SystemMetadata::SYSTEM_KEY_CONFIGURATION_ID => $this->configId,
        ];

        if ($this->configRowId) {
            $metadata[SystemMetadata::SYSTEM_KEY_CONFIGURATION_ROW_ID] = $this->configRowId;
        }

        return $metadata;
    }

    public function buildTableMetadata(): array
    {
        $metadata = $this->buildCommonMetadata();

        if ($this->clientWrapper->isDevelopmentBranch()) {
            $metadata[SystemMetadata::SYSTEM_KEY_BRANCH_ID] = $this->clientWrapper->getBranchId();
        }

        return $metadata;
    }

    public function buildFilesystemMetadata(): array
    {
        $metadata = $this->buildCommonMetadata();
        $metadata[SystemMetadata::SYSTEM_KEY_RUN_ID] = $this->clientWrapper->getBranchClient()->getRunId();
        return $metadata;
    }

    public function buildUploadTableOptions(Output $outputStorageConfig): array
    {
        $uploadTablesOptions = [
            'mapping' => $outputStorageConfig->tables->toArray(),
        ];

        // Get default bucket
        $defaultBucketName = $outputStorageConfig->defaultBucket ?? '';
        if ($defaultBucketName === '') {
            $defaultBucketName = $this->getDefaultBucket($this->component, $this->configId);
        }

        if ($defaultBucketName) {
            $uploadTablesOptions['bucket'] = $defaultBucketName;
            $this->logger->debug('Default bucket ' . $uploadTablesOptions['bucket']);
        }

        $treatValuesAsNull = $this->configuration->storage->output->treatValuesAsNull;
        if ($treatValuesAsNull !== null) {
            $uploadTablesOptions['treat_values_as_null'] = $treatValuesAsNull;
        }

        return $uploadTablesOptions;
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
}
