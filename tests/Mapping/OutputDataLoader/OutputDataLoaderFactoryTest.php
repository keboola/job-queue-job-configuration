<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\OutputDataLoader;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\Mapping\DataLoader\OutputDataLoader;
use Keboola\JobQueue\JobConfiguration\Tests\ReflectionPropertyAccessTestCase;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\TableLoader;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApiBranch\ClientWrapper;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class OutputDataLoaderFactoryTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testCreate(): void
    {
        $logger = new Logger('test');

        $component = new ComponentSpecification([
            'id' => 'my.component',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'configuration_format' => 'yaml',
                'staging_storage' => [
                    'input' => 's3',
                    'output' => 'local',
                ],
            ],
        ]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $configuration = new Configuration();

        $dataLoader = OutputDataLoader::create(
            $logger,
            $clientWrapper,
            $component,
            $configuration,
            'configId',
            'configRowId',
            null,
            '/dataDir/path',
            '/subpath/',
        );

        $expectedStagingProvider = new StagingProvider(
            StagingType::Local,
            '/dataDir/path',
            null,
        );

        $expectedStrategyFactory = new StrategyFactory(
            $expectedStagingProvider,
            $clientWrapper,
            $logger,
            FileFormat::Yaml,
        );

        $expectedFileWriter = new FileWriter(
            $clientWrapper,
            $logger,
            $expectedStrategyFactory,
        );

        $expectedTableLoader = new TableLoader(
            $logger,
            $clientWrapper,
            $expectedStrategyFactory,
        );

        self::assertEquals(
            $expectedFileWriter,
            self::getPrivatePropertyValue($dataLoader, 'fileWriter'),
        );
        self::assertEquals(
            $expectedTableLoader,
            self::getPrivatePropertyValue($dataLoader, 'tableLoader'),
        );
        self::assertSame(
            $clientWrapper,
            self::getPrivatePropertyValue($dataLoader, 'clientWrapper'),
        );
        self::assertSame(
            $component,
            self::getPrivatePropertyValue($dataLoader, 'component'),
        );
        self::assertSame(
            $configuration,
            self::getPrivatePropertyValue($dataLoader, 'configuration'),
        );
        self::assertSame(
            'configId',
            self::getPrivatePropertyValue($dataLoader, 'configId'),
        );
        self::assertSame(
            'configRowId',
            self::getPrivatePropertyValue($dataLoader, 'configRowId'),
        );
        self::assertSame(
            $logger,
            self::getPrivatePropertyValue($dataLoader, 'logger'),
        );
        self::assertSame(
            '/subpath',
            self::getPrivatePropertyValue($dataLoader, 'sourceDataDirPath'),
        );
    }
}
