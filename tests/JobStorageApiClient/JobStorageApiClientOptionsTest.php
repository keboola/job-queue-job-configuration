<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobStorageApiClient;

use InvalidArgumentException;
use Keboola\JobQueue\JobConfiguration\JobStorageApiClient\JobStorageApiClientOptions;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

/**
 * @phpstan-import-type ArrayOptions from JobStorageApiClientOptions
 */
class JobStorageApiClientOptionsTest extends TestCase
{
    public static function provideConfigDefinitionTestData(): iterable
    {
        yield 'minimum data' => [
            'data' => [
                'run_id' => 'job-123',
                'branch_id' => 'branch-123',
                'use_branch_storage' => true,
                'backend' => [],
            ],
            'expectedError' => null,
            'expectedResult' => [
                'run_id' => 'job-123',
                'branch_id' => 'branch-123',
                'use_branch_storage' => true,
                'backend' => [
                    'size' => null,
                    'context' => null,
                ],
                'backoff_max_tries' => null,
            ],
        ];

        yield 'null optional values' => [
            'data' => [
                'run_id' => 'job-123',
                'branch_id' => 'branch-123',
                'use_branch_storage' => true,
                'backoff_max_tries' => null,
                'backend' => [
                    'size' => null,
                    'context' => null,
                ],
            ],
            'expectedError' => null,
            'expectedResult' => [
                'run_id' => 'job-123',
                'branch_id' => 'branch-123',
                'use_branch_storage' => true,
                'backend' => [
                    'size' => null,
                    'context' => null,
                ],
                'backoff_max_tries' => null,
            ],
        ];

        yield 'missing run_id in storage client config' => [
            'data' => [
                'branch_id' => 'branch-123',
                'use_branch_storage' => true,
                'backend' => [
                    'size' => 'small',
                    'context' => 'context-123',
                ],
            ],
            'expectedError' => 'The child config "run_id" under "storage_client_options" must be configured',
            'expectedResult' => null,
        ];

        yield 'missing branch_id in storage client config' => [
            'data' => [
                'run_id' => 'job-123',
                'use_branch_storage' => true,
                'backend' => [
                    'size' => 'small',
                    'context' => 'context-123',
                ],
            ],
            'expectedError' => 'The child config "branch_id" under "storage_client_options" must be configured.',
            'expectedResult' => null,
        ];

        yield 'missing use_branch_storage in storage client config' => [
            'data' => [
                'run_id' => 'job-123',
                'branch_id' => 'branch-123',
                'backend' => [
                    'size' => 'small',
                    'context' => 'context-123',
                ],
            ],
            'expectedError' => 'The child config "use_branch_storage" under "storage_client_options" ' .
                'must be configured.',
            'expectedResult' => null,
        ];

        yield 'missing backend in storage client config' => [
            'data' => [
                'run_id' => 'job-123',
                'branch_id' => 'branch-123',
                'use_branch_storage' => false,
            ],
            'expectedError' => 'The child config "backend" under "storage_client_options" must be configured.',
            'expectedResult' => null,
        ];

        yield 'backoff_max_tries not integer' => [
            'data' => [
                'run_id' => 'job-123',
                'branch_id' => 'branch-123',
                'use_branch_storage' => false,
                'backoff_max_tries' => '10',
            ],
            'expectedError' => 'Invalid type for path "storage_client_options.backoff_max_tries". ' .
                'Expected "int", but got "string".',
            'expectedResult' => null,
        ];

        yield 'backoff_max_tries less than zero' => [
            'data' => [
                'run_id' => 'job-123',
                'branch_id' => 'branch-123',
                'use_branch_storage' => false,
                'backoff_max_tries' => -1,
            ],
            'expectedError' => 'The value -1 is too small for path "storage_client_options.backoff_max_tries". ' .
                'Should be greater than or equal to 0',
            'expectedResult' => null,
        ];
    }

    /** @dataProvider provideConfigDefinitionTestData */
    public function testConfigDefinition(array $data, ?string $expectedError, ?array $expectedResult): void
    {
        if ($expectedError !== null) {
            $this->expectException(InvalidConfigurationException::class);
            $this->expectExceptionMessage($expectedError);
        }

        $result = (new Processor())->process(JobStorageApiClientOptions::configDefinition()->getNode(), [
            'storage_client_options' => $data,
        ]);

        self::assertEquals($expectedResult, $result);
    }

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
