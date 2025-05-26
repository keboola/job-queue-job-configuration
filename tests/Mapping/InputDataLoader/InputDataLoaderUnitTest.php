<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Table\Result as InputTableResult;
use Keboola\JobQueue\JobConfiguration\Exception\UserException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Keboola\JobQueue\JobConfiguration\Mapping\DataLoader\InputDataLoader;
use Keboola\StorageApi\ClientException;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class InputDataLoaderUnitTest extends TestCase
{
    private readonly LoggerInterface $logger;
    private readonly TestHandler $logsHandler;

    public function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->logger = new Logger('test', [$this->logsHandler]);
    }

    public function testLoadInputDataWithEmptyConfiguration(): void
    {
        $reader = $this->createMock(Reader::class);
        $reader->expects($this->never())->method('downloadTables');
        $reader->expects($this->never())->method('downloadFiles');

        $component = new ComponentSpecification([
            'id' => 'test-component',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'test/test',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ]);
        $jobConfiguration = new Configuration();
        $jobState = new State();

        $inputDataLoader = new InputDataLoader(
            $reader,
            $component,
            $jobConfiguration,
            $jobState,
            $this->logger,
            'in/',
        );

        $result = $inputDataLoader->loadInputData();

        $expectedInputTableResult = new InputTableResult();
        $expectedInputTableResult->setInputTableStateList(new InputTableStateList([]));
        $this->assertEquals($expectedInputTableResult, $result->inputTableResult);

        $expectedInputFileStateList = new InputFileStateList([]);
        $this->assertEquals($expectedInputFileStateList, $result->inputFileStateList);

        self::assertFalse($this->logsHandler->hasDebug('Downloading source tables.'));
        self::assertFalse($this->logsHandler->hasDebug('Downloading source files.'));
    }

    public function testLoadInputDataWithTablesAndFiles(): void
    {
        $component = new ComponentSpecification([
            'id' => 'test-component',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'test/test',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ]);

        $inputTablesConfig = [
            [
                'source' => 'in.c-main.test',
                'destination' => 'test.csv',
            ],
            [
                'source' => 'in.c-main.test2',
                'destination' => 'test2.csv',
            ],
        ];

        $inputFilesConfig = [
            [
                'tags' => ['test-tag'],
                'overwrite' => true,
            ],
        ];

        $jobConfiguration = Configuration::fromArray([
            'storage' => [
                'input' => [
                    'tables' => $inputTablesConfig,
                    'files' => $inputFilesConfig,
                ],
            ],
        ]);

        $inputTablesSate = [
            [
                'source' => 'in.c-main.test',
                'lastImportDate' => '2025-01-01T00:00:00Z',
            ],
        ];

        $inputFilesState = [
            'tags' => [
                'tags' => [
                    [
                        'name' => 'test-tag',
                    ],
                ],
                'lastImportId' => '1234',
            ],
        ];

        $jobState = State::fromArray([
            'storage' => [
                'input' => [
                    'tables' => $inputTablesSate,
                    'files' =>  $inputFilesState,
                ],
            ],
        ]);

        $tablesResult = new InputTableResult();
        $tablesResult->setInputTableStateList(new InputTableStateList([]));

        $filesResult = new InputFileStateList([]);

        $reader = $this->createMock(Reader::class);
        $reader->expects($this->once())
            ->method('downloadTables')
            ->with(
                new InputTableOptionsList($inputTablesConfig),
                new InputTableStateList($inputTablesSate),
                '/in/tables/',
                new ReaderOptions(
                    devInputsDisabled: true,
                    preserveWorkspace: false,
                ),
            )
            ->willReturn($tablesResult)
        ;
        $reader->expects($this->once())
            ->method('downloadFiles')
            ->with(
                $inputFilesConfig,
                '/in/files/',
                new InputFileStateList($inputFilesState),
            )
            ->willReturn($filesResult)
        ;

        $inputDataLoader = new InputDataLoader(
            $reader,
            $component,
            $jobConfiguration,
            $jobState,
            $this->logger,
            'in/',
        );

        $result = $inputDataLoader->loadInputData();

        $this->assertSame($tablesResult, $result->inputTableResult);
        $this->assertSame($filesResult, $result->inputFileStateList);

        self::assertTrue($this->logsHandler->hasDebug('Downloading source tables.'));
        self::assertTrue($this->logsHandler->hasDebug('Downloading source files.'));
    }

    public function testLoadInputDataWithClientException(): void
    {
        $reader = $this->createMock(Reader::class);
        $reader->expects($this->once())
            ->method('downloadTables')
            ->willThrowException(new ClientException('Storage API error'));

        $component = new ComponentSpecification([
            'id' => 'test-component',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'test/test',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ]);
        $jobConfiguration = Configuration::fromArray([
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.test',
                            'destination' => 'test.csv',
                        ],
                    ],
                ],
            ],
        ]);
        $jobState = new State();

        // Use InputDataLoader with the mock reader
        $inputDataLoader = new InputDataLoader(
            $reader,
            $component,
            $jobConfiguration,
            $jobState,
            $this->logger,
            'in/',
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Cannot import data from Storage API: Storage API error');

        $inputDataLoader->loadInputData();
    }

    public function testLoadInputDataWithInvalidInputException(): void
    {
        $reader = $this->createMock(Reader::class);
        $reader->expects($this->once())
            ->method('downloadTables')
            ->willThrowException(new InvalidInputException('Invalid input configuration'));

        $component = new ComponentSpecification([
            'id' => 'test-component',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'test/test',
                ],
                'staging-storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
        ]);
        $jobConfiguration = Configuration::fromArray([
            'storage' => [
                'input' => [
                    'tables' => [
                        [
                            'source' => 'in.c-main.test',
                            'destination' => 'test.csv',
                        ],
                    ],
                ],
            ],
        ]);
        $jobState = new State();

        // Use InputDataLoader with the mock reader
        $inputDataLoader = new InputDataLoader(
            $reader,
            $component,
            $jobConfiguration,
            $jobState,
            $this->logger,
            'in/',
        );

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Invalid input configuration');

        $inputDataLoader->loadInputData();
    }
}
