<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobStorageApiClient;

use Keboola\JobQueue\JobConfiguration\JobStorageApiClient\JobStorageApiClientFactory;
use Keboola\JobQueue\JobConfiguration\JobStorageApiClient\JobStorageApiClientOptions;
use Keboola\StorageApi\Options\BackendConfiguration;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use PHPUnit\Framework\Constraint\Constraint;
use PHPUnit\Framework\TestCase;

class JobStorageApiClientFactoryTest extends TestCase
{
    public static function provideCreateClientWrapperTestData(): iterable
    {
        yield 'basic job' => [
            'parentClientOptions' => new ClientOptions(),
            'constructorClientOptions' => new ClientOptions(),
            'jobClientOptions' => new JobStorageApiClientOptions(
                runId: 'job-123',
                branchId: 'branch-123',
                useBranchStorage: false,
            ),
            'expectedClientOptions' => new ClientOptions(
                branchId: 'branch-123',
                runId: 'job-123',
                userAgent: '(job: job-123)',
                backoffMaxTries: 20,
                backendConfiguration: new BackendConfiguration(),
                useBranchStorage: false,
            ),
        ];

        yield 'custom retries count' => [
            'parentClientOptions' => new ClientOptions(),
            'constructorClientOptions' => new ClientOptions(
                backoffMaxTries: 50,
            ),
            'jobClientOptions' => new JobStorageApiClientOptions(
                runId: 'job-123',
                branchId: 'branch-123',
                useBranchStorage: false,
                backoffMaxTries: 100,
            ),
            'expectedClientOptions' => new ClientOptions(
                branchId: 'branch-123',
                runId: 'job-123',
                userAgent: '(job: job-123)',
                backoffMaxTries: 100,
                backendConfiguration: new BackendConfiguration(),
                useBranchStorage: false,
            ),
        ];

        yield 'custom retries from client options' => [
            'parentClientOptions' => new ClientOptions(),
            'constructorClientOptions' => new ClientOptions(
                backoffMaxTries: 50,
            ),
            'jobClientOptions' => new JobStorageApiClientOptions(
                runId: 'job-123',
                branchId: 'branch-123',
                useBranchStorage: false,
            ),
            'expectedClientOptions' => new ClientOptions(
                branchId: 'branch-123',
                runId: 'job-123',
                userAgent: '(job: job-123)',
                backoffMaxTries: 50,
                backendConfiguration: new BackendConfiguration(),
                useBranchStorage: false,
            ),
        ];

        yield 'job with protected-default-branch feature' => [
            'parentClientOptions' => new ClientOptions(),
            'constructorClientOptions' => new ClientOptions(),
            'jobClientOptions' => new JobStorageApiClientOptions(
                runId: 'job-123',
                branchId: 'branch-123',
                useBranchStorage: true,
            ),
            'expectedClientOptions' => new ClientOptions(
                branchId: 'branch-123',
                runId: 'job-123',
                userAgent: '(job: job-123)',
                backoffMaxTries: 20,
                backendConfiguration: new BackendConfiguration(),
                useBranchStorage: true,
            ),
        ];

        yield 'job with staging workspace' => [
            'parentClientOptions' => new ClientOptions(),
            'constructorClientOptions' => new ClientOptions(),
            'jobClientOptions' => new JobStorageApiClientOptions(
                runId: 'job-123',
                branchId: 'branch-123',
                useBranchStorage: false,
                backendSize: 'small',
                backendContext: '123-transformation',
            ),
            'expectedClientOptions' => new ClientOptions(
                branchId: 'branch-123',
                runId: 'job-123',
                userAgent: '(job: job-123)',
                backoffMaxTries: 20,
                backendConfiguration: new BackendConfiguration(
                    context: '123-transformation',
                    size: 'small',
                ),
                useBranchStorage: false,
            ),
        ];

        yield 'parameters in parent client options' => [
            'parentClientOptions' => new ClientOptions(
                url: 'https://example.com',
                token: 'token',
                userAgent: 'My Client',
            ),
            'constructorClientOptions' => new ClientOptions(),
            'jobClientOptions' => new JobStorageApiClientOptions(
                runId: 'job-123',
                branchId: 'branch-123',
                useBranchStorage: false,
            ),
            'expectedClientOptions' => new ClientOptions(
                branchId: 'branch-123',
                runId: 'job-123',
                userAgent: 'My Client (job: job-123)',
                backoffMaxTries: 20,
                backendConfiguration: new BackendConfiguration(),
                useBranchStorage: false,
            ),
        ];

        yield 'parameters in constructor client options' => [
            'parentClientOptions' => new ClientOptions(
                url: 'https://example.com',
                token: 'token',
                userAgent: 'My Client',
            ),
            'constructorClientOptions' => new ClientOptions(
                url: 'https://example-1.com',
                branchId: 'branch-789',
                userAgent: 'My Client 2',
            ),
            'jobClientOptions' => new JobStorageApiClientOptions(
                runId: 'job-123',
                branchId: 'branch-789',
                useBranchStorage: false,
            ),
            'expectedClientOptions' => new ClientOptions(
                url: 'https://example-1.com',
                branchId: 'branch-789',
                runId: 'job-123',
                userAgent: 'My Client 2 (job: job-123)',
                backoffMaxTries: 20,
                backendConfiguration: new BackendConfiguration(),
                useBranchStorage: false,
            ),
        ];
    }

    /** @dataProvider provideCreateClientWrapperTestData */
    public function testCreateClientWrapper(
        ClientOptions $parentClientOptions,
        ClientOptions $constructorClientOptions,
        JobStorageApiClientOptions $jobClientOptions,
        ClientOptions $expectedClientOptions,
    ): void {
        $createdClientWrapper = $this->createMock(ClientWrapper::class);

        $parentFactory = $this->createMock(StorageClientPlainFactory::class);
        $parentFactory
            ->method('getClientOptionsReadOnly')
            ->willReturn($parentClientOptions)
        ;
        $parentFactory->expects(self::once())
            ->method('createClientWrapper')
            ->with(self::clientOptionsEquals($expectedClientOptions))
            ->willReturn($createdClientWrapper)
        ;

        $factory = new JobStorageApiClientFactory($parentFactory, $constructorClientOptions);
        $result = $factory->createClientWrapper($jobClientOptions);

        self::assertSame($createdClientWrapper, $result);
    }

    private static function clientOptionsEquals(ClientOptions $expectedValue): Constraint
    {
        return self::callback(function (ClientOptions $actualValue) use ($expectedValue): bool {
            $actualValue = clone $actualValue;

            // closure can't be validated directly, just check it's present
            $jobPollRetryDelay = $actualValue->getJobPollRetryDelay();
            self::assertNotNull($jobPollRetryDelay);
            self::assertJobPollRetryDelayCallbackIsValid($jobPollRetryDelay);

            $actualValue->setJobPollRetryDelay(null);
            self::assertEquals($expectedValue, $actualValue);
            return true;
        });
    }

    private static function assertJobPollRetryDelayCallbackIsValid(callable $callback): void
    {
        self::assertSame(1, $callback(0));
        self::assertSame(1, $callback(1));
        self::assertSame(1, $callback(14));
        self::assertSame(2, $callback(15));
        self::assertSame(2, $callback(29));
        self::assertSame(5, $callback(30));
        self::assertSame(5, $callback(1000));
    }
}
