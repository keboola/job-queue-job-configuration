<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Output;
use Keboola\JobQueue\JobConfiguration\Mapping\DataLoader\OutputDataLoader;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\TableLoader;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class OutputDataLoaderUnitTest extends TestCase
{
    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    public function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('test', [$this->logsHandler]);
    }

    private function createClientWrapperMock(
        array $features = [],
        ?string $devBranchId = null,
    ): ClientWrapper {
        $token = $this->createMock(StorageApiToken::class);
        $token->method('hasFeature')->willReturnCallback(
            fn(string $feature) => in_array($feature, $features, true),
        );

        $branchClient = $this->createMock(BranchAwareClient::class);
        $branchClient->method('getRunId')->willReturn('test-run-id');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getToken')->willReturn($token);
        $clientWrapper->method('isDevelopmentBranch')->willReturn($devBranchId !== null);
        $clientWrapper->method('getBranchId')->willReturn($devBranchId ?? 'prod-branch-id');
        $clientWrapper->method('getBranchClient')->willReturn($branchClient);

        return $clientWrapper;
    }

    public function testStoreOutputWithEmptyConfiguration(): void
    {
        $fileWriter = $this->createMock(FileWriter::class);
        $fileWriter->expects($this->once())
            ->method('uploadFiles')
            ->with(
                '/out/files/',
                ['mapping' => []],
                $this->anything(),
                [],
                false,
            );

        $createdLoadTableQueue = $this->createMock(LoadTableQueue::class);

        $tableLoader = $this->createMock(TableLoader::class);
        $tableLoader->expects($this->once())
            ->method('uploadTables')
            ->willReturn($createdLoadTableQueue);

        $clientWrapper = $this->createClientWrapperMock();
        $component = $this->createComponentMock();
        $jobConfiguration = new Configuration();

        $outputDataLoader = new OutputDataLoader(
            $fileWriter,
            $tableLoader,
            $clientWrapper,
            $component,
            $jobConfiguration,
            null,
            null,
            $this->logger,
            'out/',
        );

        $result = $outputDataLoader->storeOutput();

        self::assertSame($createdLoadTableQueue, $result);

        self::assertTrue($this->logsHandler->hasDebug('Storing results.'));
        self::assertTrue($this->logsHandler->hasDebug('Uploading output tables and files.'));
    }

    public function testStoreOutputWithTablesAndFiles(): void
    {
        $outputTablesConfig = [
            [
                'source' => 'test.csv',
                'destination' => 'out.c-main.test',
            ],
            [
                'source' => 'test2.csv',
                'destination' => 'out.c-main.test2',
            ],
        ];

        $outputFilesConfig = [
            [
                'source' => 'file.txt',
                'tags' => ['test-tag'],
            ],
        ];

        $inputFilesConfig = [
            [
                'tags' => ['input-tag'],
                'overwrite' => true,
            ],
        ];

        $fileWriter = $this->createMock(FileWriter::class);
        $fileWriter->expects($this->once())
            ->method('uploadFiles')
            ->with(
                '/out/files/',
                $this->anything(),
                $this->anything(),
                [],
                false,
            );

        $fileWriter->expects($this->once())
            ->method('tagFiles')
            ->with($inputFilesConfig);

        $createdLoadTableQueue = $this->createMock(LoadTableQueue::class);

        $tableLoader = $this->createMock(TableLoader::class);
        $tableLoader->expects($this->once())
            ->method('uploadTables')
            ->willReturn($createdLoadTableQueue);

        $clientWrapper = $this->createClientWrapperMock();
        $component = $this->createComponentMock();

        $jobConfiguration = Configuration::fromArray([
            'storage' => [
                'input' => [
                    'files' => $inputFilesConfig,
                ],
                'output' => [
                    'tables' => $outputTablesConfig,
                    'files' => $outputFilesConfig,
                ],
            ],
        ]);

        $outputDataLoader = new OutputDataLoader(
            $fileWriter,
            $tableLoader,
            $clientWrapper,
            $component,
            $jobConfiguration,
            null,
            null,
            $this->logger,
            'out/',
        );

        $result = $outputDataLoader->storeOutput();

        $this->assertSame($createdLoadTableQueue, $result);

        self::assertTrue($this->logsHandler->hasDebug('Storing results.'));
        self::assertTrue($this->logsHandler->hasDebug('Uploading output tables and files.'));
    }

    public function testStoreOutputWithInvalidOutputException(): void
    {
        $fileWriter = $this->createMock(FileWriter::class);
        $fileWriter->expects($this->once())
            ->method('uploadFiles')
            ->willThrowException(new InvalidOutputException('Invalid output configuration'));

        $tableLoader = $this->createMock(TableLoader::class);
        $tableLoader->expects($this->never())
            ->method('uploadTables');

        $clientWrapper = $this->createClientWrapperMock();
        $component = $this->createComponentMock();
        $jobConfiguration = new Configuration();

        $outputDataLoader = new OutputDataLoader(
            $fileWriter,
            $tableLoader,
            $clientWrapper,
            $component,
            $jobConfiguration,
            null,
            null,
            $this->logger,
            'out/',
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Invalid output configuration');

        $outputDataLoader->storeOutput();
    }

    public static function provideFailedJobFlag(): iterable
    {
        yield 'isFailedJob: false' => [false];
        yield 'isFailedJob: true' => [true];
    }

    /** @dataProvider provideFailedJobFlag */
    public function testStoreOutputWithFileStorageOnly(bool $isFailedJob): void
    {
        // fileStorageOnly feature is not directly related to isFailedJobFlag, but it's convenient to test together
        // as it covers bot uses of the isFailedJob flag

        $outputTablesConfig = [
            [
                'source' => 'test.csv',
                'destination' => 'out.c-main.test',
            ],
        ];

        $outputFilesConfig = [
            [
                'source' => 'file.txt',
                'tags' => ['test-tag'],
            ],
        ];

        $inputFilesConfig = [
            [
                'tags' => ['input-tag'],
                'overwrite' => true,
            ],
        ];

        $uploadFilesCounter = $this->exactly(2);
        $fileWriter = $this->createMock(FileWriter::class);
        $fileWriter->expects($uploadFilesCounter)
            ->method('uploadFiles')
            ->willReturnCallback(
                function ($path, $mapping, $metadata, $tableFiles, $isFailed) use ($uploadFilesCounter, $isFailedJob) {
                    switch ($uploadFilesCounter->numberOfInvocations()) {
                        case 1:
                            self::assertSame('/out/files/', $path);
                            self::assertSame(
                                [
                                    'mapping' => [
                                        [
                                            'source' => 'file.txt',
                                            'tags' => ['test-tag'],
                                            'is_public' => false,
                                            'is_permanent' => false,
                                            'is_encrypted' => true,
                                            'notify' => false,
                                        ],
                                    ],
                                ],
                                $mapping,
                            );
                            self::assertSame(
                                [
                                    'componentId' => 'test-component',
                                    'configurationId' => null,
                                    'runId' => 'test-run-id',
                                ],
                                $metadata,
                            );
                            self::assertSame([], $tableFiles);
                            self::assertSame($isFailedJob, $isFailed);
                            break;

                        case 2:
                            self::assertSame('/out/tables/', $path);
                            self::assertSame([], $mapping);
                            self::assertSame(
                                [
                                    'componentId' => 'test-component',
                                    'configurationId' => null,
                                    'runId' => 'test-run-id',
                                ],
                                $metadata,
                            );
                            self::assertSame(
                                [
                                    'tags' => [],
                                    'is_permanent' => true,
                                ],
                                $tableFiles,
                            );
                            self::assertSame($isFailedJob, $isFailed);
                            break;
                    }
                    return null;
                },
            );

        $fileWriter->expects($this->once())
            ->method('tagFiles')
            ->with($inputFilesConfig);

        $tableLoader = $this->createMock(TableLoader::class);
        $tableLoader->expects($this->never())
            ->method('uploadTables');

        $clientWrapper = $this->createClientWrapperMock();
        $component = $this->createComponentMock(['allow-use-file-storage-only']);

        $jobConfiguration = Configuration::fromArray([
            'storage' => [
                'input' => [
                    'files' => $inputFilesConfig,
                ],
                'output' => [
                    'tables' => $outputTablesConfig,
                    'files' => $outputFilesConfig,
                ],
            ],
            'runtime' => [
                'use_file_storage_only' => true,
            ],
        ]);

        $outputDataLoader = new OutputDataLoader(
            $fileWriter,
            $tableLoader,
            $clientWrapper,
            $component,
            $jobConfiguration,
            null,
            null,
            $this->logger,
            'out/',
        );

        $result = $outputDataLoader->storeOutput($isFailedJob);

        $this->assertNull($result);
        self::assertTrue($this->logsHandler->hasDebug('Storing results.'));
        self::assertTrue($this->logsHandler->hasDebug('Uploading output tables and files.'));
    }

    private function createComponentMock(
        array $features = [],
        bool $withDefaultBucket = false,
    ): ComponentSpecification {
        $data = [
            'definition' => [
                'type' => 'dockerhub',
                'uri' => 'test/test',
            ],
            'staging-storage' => [
                'input' => 'local',
                'output' => 'local',
            ],
        ];

        if ($withDefaultBucket) {
            $data['default_bucket'] = true;
            $data['default_bucket_stage'] = 'in';
        }

        return new ComponentSpecification([
            'id' => 'test-component',
            'data' => $data,
            'features' => $features,
        ]);
    }

    public static function defaultBucketProvider(): array
    {
        return [
            'Component has no default bucket, no config default bucket' => [
                'componentHasDefaultBucket' => false,
                'configDefaultBucket' => null,
                'expectedBucket' => null,
            ],
            'Component has default bucket, no config default bucket' => [
                'componentHasDefaultBucket' => true,
                'configDefaultBucket' => null,
                'expectedBucket' => 'in.c-test-component-test-config-id',
            ],
            'Component has default bucket, config has default bucket' => [
                'componentHasDefaultBucket' => true,
                'configDefaultBucket' => 'in.c-custom-bucket',
                'expectedBucket' => 'in.c-custom-bucket',
            ],
            'Component has no default bucket, config has default bucket' => [
                'componentHasDefaultBucket' => false,
                'configDefaultBucket' => 'in.c-custom-bucket',
                'expectedBucket' => 'in.c-custom-bucket',
            ],
        ];
    }

    /** @dataProvider defaultBucketProvider */
    public function testDefaultBucketUsage(
        bool $componentHasDefaultBucket,
        ?string $configDefaultBucket,
        ?string $expectedBucket,
    ): void {
        $tableLoader = $this->createMock(TableLoader::class);
        $tableLoader->expects($this->once())
            ->method('uploadTables')
            ->with(
                $this->callback(fn(OutputMappingSettings $mappingSettings) =>
                    $mappingSettings->getDefaultBucket() === $expectedBucket,),
            )
            ->willReturn($this->createMock(LoadTableQueue::class));

        $fileWriter = $this->createMock(FileWriter::class);
        $fileWriter->expects($this->once())
            ->method('uploadFiles');

        $clientWrapper = $this->createClientWrapperMock();
        $component = $this->createComponentMock([], $componentHasDefaultBucket);

        $jobConfiguration = Configuration::fromArray([
            'storage' => [
                'output' => [
                    'default_bucket' => $configDefaultBucket,
                ],
            ],
        ]);

        $outputDataLoader = new OutputDataLoader(
            $fileWriter,
            $tableLoader,
            $clientWrapper,
            $component,
            $jobConfiguration,
            'test-config-id',
            null,
            $this->logger,
            'out/',
        );

        $outputDataLoader->storeOutput();

        // expect the log only if the default bucket should be used
        self::assertSame(
            $expectedBucket !== null,
            $this->logsHandler->hasDebug('Default bucket ' . $expectedBucket),
        );
    }

    public function testDefaultBucketWithoutConfigId(): void
    {
        $tableLoader = $this->createMock(TableLoader::class);
        $tableLoader->expects($this->never())
            ->method('uploadTables');

        $fileWriter = $this->createMock(FileWriter::class);
        $fileWriter->expects($this->once())
            ->method('uploadFiles');

        $clientWrapper = $this->createClientWrapperMock();
        $component = $this->createComponentMock([], true); // Component with default bucket

        $jobConfiguration = new Configuration();

        $outputDataLoader = new OutputDataLoader(
            $fileWriter,
            $tableLoader,
            $clientWrapper,
            $component,
            $jobConfiguration,
            null, // No configId
            null,
            $this->logger,
            'out/',
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Configuration ID not set, but is required for default_bucket option.');

        $outputDataLoader->storeOutput();
    }

    public function testTreatValuesAsNull(): void
    {
        $treatValuesAsNull = ['foo', 'bar'];

        $tableLoader = $this->createMock(TableLoader::class);
        $tableLoader->expects($this->once())
            ->method('uploadTables')
            ->with(
                $this->callback(fn(OutputMappingSettings $mappingSettings) =>
                    $mappingSettings->getTreatValuesAsNull() === $treatValuesAsNull,),
            )
            ->willReturn($this->createMock(LoadTableQueue::class));

        $fileWriter = $this->createMock(FileWriter::class);
        $fileWriter->expects($this->once())
            ->method('uploadFiles');

        $clientWrapper = $this->createClientWrapperMock();
        $component = $this->createComponentMock();

        $jobConfiguration = Configuration::fromArray([
            'storage' => [
                'output' => [
                    'treat_values_as_null' => $treatValuesAsNull,
                ],
            ],
        ]);

        $outputDataLoader = new OutputDataLoader(
            $fileWriter,
            $tableLoader,
            $clientWrapper,
            $component,
            $jobConfiguration,
            'test-config-id',
            null,
            $this->logger,
            'out/',
        );

        $outputDataLoader->storeOutput();
    }
}
