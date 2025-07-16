<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobStorageApiClient;

use InvalidArgumentException;
use Keboola\JobQueue\JobConfiguration\JobStorageApiClient\JobStorageApiClientOptions;
use PHPUnit\Framework\TestCase;

/**
 * @phpstan-import-type ArrayOptions from JobStorageApiClientOptions
 */
class JobStorageApiClientOptionsTest extends TestCase
{
    public static function provideFromArrayTestData(): iterable
    {
        yield 'minimal data' => [
            'data' => [
                'run_id' => 'run-id',
                'branch_id' => 'branch-id',
                'use_branch_storage' => false,
            ],
            'expectedResult' => new JobStorageApiClientOptions(
                runId: 'run-id',
                branchId: 'branch-id',
                useBranchStorage: false,
                backoffMaxTries: null,
                backendSize: null,
                backendContext: null,
            ),
        ];

        yield 'all data' => [
            'data' => [
                'run_id' => 'run-id',
                'branch_id' => 'branch-id',
                'use_branch_storage' => true,
                'backoff_max_tries' => 10,
                'backend' => [
                    'size' => 'small',
                    'context' => '123-transformation',
                ],
            ],
            'expectedResult' => new JobStorageApiClientOptions(
                runId: 'run-id',
                branchId: 'branch-id',
                useBranchStorage: true,
                backoffMaxTries: 10,
                backendSize: 'small',
                backendContext: '123-transformation',
            ),
        ];
    }

    /**
     * @dataProvider provideFromArrayTestData
     * @param ArrayOptions $data
     */
    public function testFromArray(array $data, JobStorageApiClientOptions $expectedResult): void
    {
        $options = JobStorageApiClientOptions::fromArray($data);
        self::assertEquals($expectedResult, $options);
    }

    public function testFromInvalidArray(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid job storage client options: ');

        // @phpstan-ignore-next-line intentionally invalid data
        JobStorageApiClientOptions::fromArray([
            'run_id' => 1,
            'branch_id' => null,
            'use_branch_storage' => 4,
        ]);
    }

    public static function provideToArrayTestData(): iterable
    {
        yield 'minimal data' => [
            'options' => new JobStorageApiClientOptions(
                runId: 'run-id',
                branchId: 'branch-id',
                useBranchStorage: false,
            ),
            'expectedArray' => [
                'run_id' => 'run-id',
                'branch_id' => 'branch-id',
                'use_branch_storage' => false,
                'backoff_max_tries' => null,
                'backend' => [
                    'size' => null,
                    'context' => null,
                ],
            ],
        ];

        yield 'all data' => [
            'options' => new JobStorageApiClientOptions(
                runId: 'run-id',
                branchId: 'branch-id',
                useBranchStorage: true,
                backoffMaxTries: 10,
                backendSize: 'small',
                backendContext: '123-transformation',
            ),
            'expectedArray' => [
                'run_id' => 'run-id',
                'branch_id' => 'branch-id',
                'use_branch_storage' => true,
                'backoff_max_tries' => 10,
                'backend' => [
                    'size' => 'small',
                    'context' => '123-transformation',
                ],
            ],
        ];
    }

    /** @dataProvider provideToArrayTestData */
    public function testToArray(JobStorageApiClientOptions $options, array $expectedArray): void
    {
        $array = $options->toArray();
        self::assertEquals($expectedArray, $array);
    }
}
