<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobStorageApiClient;

use Keboola\StorageApi\Options\BackendConfiguration;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

class JobStorageApiClientFactory
{
    private const DEFAULT_BACKOFF_MAX_TRIES = 20;

    public function __construct(
        private readonly StorageClientPlainFactory $parentFactory,
        private readonly ClientOptions $clientOptions = new ClientOptions(),
    ) {
    }

    public function createClientWrapper(JobStorageApiClientOptions $jobClientOptions): ClientWrapper
    {
        $clientOptions = $this->createClientOptionsFromArrayOptions($jobClientOptions);
        return $this->parentFactory->createClientWrapper($clientOptions);
    }

    private function createClientOptionsFromArrayOptions(JobStorageApiClientOptions $options): ClientOptions
    {
        $userAgentPrefix =
            $this->clientOptions->getUserAgent() ??
            $this->parentFactory->getClientOptionsReadOnly()->getUserAgent() ??
            ''
        ;

        // Here we intentionally set client.runId to job.jobId because it no longer holds that
        // storage-api-run-id is the same as job-queue-run-id. storage-api-run-id now serves as a tracing id
        // attributing storage operations to job. job-queue-run-id describes the hierarchical structure of jobs.
        // By using jobId as storage runId we're attributing storage operations to that job (and not its parents).
        // Technically, the reason is that storage api run id has a limited length and cannot be easily extended.
        return (clone $this->clientOptions)
            ->setUserAgent(trim(sprintf('%s (job: %s)', $userAgentPrefix, $options->runId)))
            ->setBranchId($options->branchId)
            ->setRunId($options->runId)
            ->setUseBranchStorage($options->useBranchStorage)
            ->setBackoffMaxTries(
                $options->backoffMaxTries ??
                $this->clientOptions->getBackoffMaxTries() ??
                self::DEFAULT_BACKOFF_MAX_TRIES,
            )
            ->setJobPollRetryDelay(
                fn($tries) => match (true) {
                    $tries < 15 => 1,
                    $tries < 30 => 2,
                    default => 5,
                },
            )
            ->setBackendConfiguration(new BackendConfiguration(
                context: $options->backendContext,
                size: $options->backendSize,
            ))
        ;
    }
}
