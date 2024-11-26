<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Generator;
use Keboola\InputMapping\Staging\AbstractStagingDefinition;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\DataTypeSupport;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Runtime;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Output;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StagingProvider\Provider\WorkspaceStagingProvider;
use Keboola\StorageApi\ClientException;
use Psr\Log\LoggerInterface;

class OutputDataLoader extends BaseDataLoader
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
        $this->validateComponentStagingSetting($component);

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
                $this->getDataTypeSupport($component, $outputStorageConfig)->value,
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

    private function getDataTypeSupport(ComponentSpecification $component, Output $outputStorageConfig): DataTypeSupport
    {
        if (!$this->outputStrategyFactory->getClientWrapper()->getToken()->hasFeature('new-native-types')) {
            return DataTypeSupport::NONE;
        }
        return $outputStorageConfig->dataTypeSupport ?? $component->getDataTypesSupport();
    }

    public function getWorkspaceBackendSize(): ?string
    {
        // this returns the first workspace found, which is ok so far because there can only be one
        // (ensured in validateStagingSetting()) working only with inputStrategyFactory, but
        // the workspace providers are shared between input and output, so it's "ok"
        foreach ($this->outputStrategyFactory->getStrategyMap() as $stagingDefinition) {
            foreach ($this->getStagingProviders($stagingDefinition) as $stagingProvider) {
                if (!$stagingProvider instanceof WorkspaceStagingProvider) {
                    continue;
                }

                return $stagingProvider->getBackendSize();
            }
        }

        return null;
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

    public function cleanWorkspace(ComponentSpecification $component, ?string $configId): void
    {
        $cleanedProviders = [];
        $maps = array_merge(
            $this->outputStrategyFactory->getStrategyMap(),
        );
        foreach ($maps as $stagingDefinition) {
            foreach ($this->getStagingProviders($stagingDefinition) as $stagingProvider) {
                if (!$stagingProvider instanceof WorkspaceStagingProvider) {
                    continue;
                }
                if (in_array($stagingProvider, $cleanedProviders, true)) {
                    continue;
                }
                /* don't clean ABS workspaces or Redshift workspaces which are reusable if created for a config.

                    The whole condition and the isReusableWorkspace method can probably be completely removed,
                    because now it is distinguished between NewWorkspaceStagingProvider (cleanup) and
                    ExistingWorkspaceStagingProvider (no cleanup).

                    However, since ABS and Redshift workspaces are not used in real life and badly tested, I don't
                    want to remove it now.
                 */
                if ($configId && $this->isReusableWorkspace($component)) {
                    continue;
                }

                try {
                    $stagingProvider->cleanup();
                    $cleanedProviders[] = $stagingProvider;
                } catch (ClientException $e) {
                    // ignore errors if the cleanup fails because we a) can't fix it b) should not break the job
                    $this->logger->error('Failed to cleanup workspace: ' . $e->getMessage());
                }
            }
        }
    }

    private function isReusableWorkspace(ComponentSpecification $component): bool
    {
        return $component->getOutputStagingStorage() === AbstractStrategyFactory::WORKSPACE_ABS;
    }
}
