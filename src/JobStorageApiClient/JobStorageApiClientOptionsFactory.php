<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobStorageApiClient;

use Keboola\JobQueueInternalClient\Client as QueueInternalApiClient;
use Keboola\JobQueueInternalClient\JobFactory\PlainJobInterface;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

class JobStorageApiClientOptionsFactory
{
    private const PROTECTED_DEFAULT_BRANCH_FEATURE = 'protected-default-branch';

    public function __construct(
        private readonly StorageClientPlainFactory $parentFactory,
        private readonly ClientOptions $clientOptions = new ClientOptions(),
    ) {
    }

    /**
     * @param QueueInternalApiClient<PlainJobInterface> $queueInternalApiClient
     */
    public function createOptionsForJobId(
        QueueInternalApiClient $queueInternalApiClient,
        string $jobId,
    ): JobStorageApiClientOptions {
        $job = $queueInternalApiClient->getJob($jobId);
        return $this->createOptionsForJob($job);
    }

    public function createOptionsForJob(PlainJobInterface $job): JobStorageApiClientOptions
    {
        $baseClient = $this->parentFactory->createClientWrapper(new ClientOptions());
        $storageApiToken = $baseClient->getToken();

        $jobId = $job->getId();
        assert($jobId !== '');

        // use resolved branchId from the base client, the job still has nullable branchId
        $branchId = $baseClient->getBranchId();
        assert($branchId !== '');

        $backoffMaxTries = $this->clientOptions->getBackoffMaxTries();
        assert($backoffMaxTries === null || $backoffMaxTries >= 0);

        return new JobStorageApiClientOptions(
            runId: $jobId,
            branchId: $branchId,
            useBranchStorage: $storageApiToken->hasFeature(self::PROTECTED_DEFAULT_BRANCH_FEATURE),
            backoffMaxTries: $backoffMaxTries,
            backendSize: $job->getBackend()->getType(),
            backendContext: $job->getBackend()->getContext(),
        );
    }
}
