<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobStorageApiClient;

use Keboola\JobQueue\JobConfiguration\JobStorageApiClient\JobStorageApiClientOptions;
use Keboola\JobQueue\JobConfiguration\JobStorageApiClient\JobStorageApiClientOptionsFactory;
use Keboola\JobQueueInternalClient\Client as QueueInternalApiClient;
use Keboola\JobQueueInternalClient\JobFactory\PlainJob;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Keboola\StorageApiBranch\StorageApiToken;
use PHPUnit\Framework\TestCase;

class JobStorageApiClientOptionsFactoryTest extends TestCase
{
    public static function provideCreateOptionsForJobTestData(): iterable
    {
        yield 'basic job' => [
            'projectFeatures' => [],
            'backendType' => null,
            'backendContext' => null,
            'expectedResult' => new JobStorageApiClientOptions(
                runId: 'job-id',
                branchId: 'branch-id',
                useBranchStorage: false,
                backoffMaxTries: null,
                backendSize: null,
                backendContext: null,
            ),
        ];

        yield 'project with protected-default-branch feature' => [
            'projectFeatures' => ['protected-default-branch'],
            'backendType' => null,
            'backendContext' => null,
            'expectedResult' => new JobStorageApiClientOptions(
                runId: 'job-id',
                branchId: 'branch-id',
                useBranchStorage: true,
                backoffMaxTries: null,
                backendSize: null,
                backendContext: null,
            ),
        ];

        yield 'job with staging workspace' => [
            'projectFeatures' => [],
            'backendType' => 'small',
            'backendContext' => '123-transformation',
            'expectedResult' => new JobStorageApiClientOptions(
                runId: 'job-id',
                branchId: 'branch-id',
                useBranchStorage: false,
                backoffMaxTries: null,
                backendSize: 'small',
                backendContext: '123-transformation',
            ),
        ];
    }

    /** @dataProvider provideCreateOptionsForJobTestData */
    public function testCreateOptionsForJob(
        array $projectFeatures,
        ?string $backendType,
        ?string $backendContext,
        JobStorageApiClientOptions $expectedResult,
    ): void {
        $job = new PlainJob([
            'id' => 'job-id', // job-id must be used as runId in options!!
            'runId' => 'run-id',
            'branchId' => null, // value is resolved through the parent storageClient, not taken directly from the job
            'branchType' => 'default',
            'status' => 'processing',
            'backend' => [
                'type' => $backendType,
                'context' => $backendContext,
            ],
        ]);

        $storageApiToken = new StorageApiToken([
            'owner' => [
                'features' => $projectFeatures,
            ],
        ], '');

        $baseClient = $this->createMock(ClientWrapper::class);
        $baseClient->expects($this->once())
            ->method('getToken')
            ->willReturn($storageApiToken)
        ;
        $baseClient->expects($this->once())
            ->method('getBranchId')
            ->willReturn('branch-id')
        ;

        $parentFactory = $this->createMock(StorageClientPlainFactory::class);
        $parentFactory->expects($this->once())
            ->method('createClientWrapper')
            ->with(new ClientOptions())
            ->willReturn($baseClient)
        ;

        $optionsFactory = new JobStorageApiClientOptionsFactory($parentFactory);
        $options = $optionsFactory->createOptionsForJob($job);

        self::assertEquals($expectedResult, $options);
    }

    public function testCreateOptionsForJobId(): void
    {
        $jobId = 'job-123';

        $job = new PlainJob([
            'branchType' => 'default',
            'status' => 'processing',
            'runId' => 'run-id',
        ]);

        $createdClientOptions = new JobStorageApiClientOptions(
            runId: 'run-id',
            branchId: 'branch-id',
            useBranchStorage: false,
        );

        $queueApiClient = $this->createMock(QueueInternalApiClient::class);
        $queueApiClient->expects(self::once())
            ->method('getJob')
            ->with($jobId)
            ->willReturn($job)
        ;

        $factory = $this->createPartialMock(JobStorageApiClientOptionsFactory::class, [
            'createOptionsForJob',
        ]);
        $factory->expects(self::once())
            ->method('createOptionsForJob')
            ->with($job)
            ->willReturn($createdClientOptions)
        ;

        $options = $factory->createOptionsForJobId($queueApiClient, $jobId);

        self::assertSame($createdClientOptions, $options);
    }
}
