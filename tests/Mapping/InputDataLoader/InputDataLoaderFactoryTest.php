<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Keboola\JobQueue\JobConfiguration\Mapping\DataLoader\InputDataLoader;
use Keboola\JobQueue\JobConfiguration\Tests\ReflectionPropertyAccessTestCase;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApiBranch\ClientWrapper;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class InputDataLoaderFactoryTest extends TestCase
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
                'configuration_format' => 'json',
                'staging_storage' => [
                    'input' => 's3',
                    'output' => 'local',
                ],
            ],
        ]);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $configuration = new Configuration();
        $state = new State();

        $dataLoader = InputDataLoader::create(
            $logger,
            $clientWrapper,
            $component,
            $configuration,
            $state,
            null,
            '/dataDir/path',
            '/subpath/',
        );

        $expectedStagingProvider = new StagingProvider(
            StagingType::S3,
            '/dataDir/path',
            null,
        );

        $expectedStrategyFactory = new StrategyFactory(
            $expectedStagingProvider,
            $clientWrapper,
            $logger,
            FileFormat::Json,
        );

        $expectedReader = new Reader(
            $clientWrapper,
            $logger,
            $expectedStrategyFactory,
        );

        self::assertEquals(
            $expectedReader,
            self::getPrivatePropertyValue($dataLoader, 'reader'),
        );
        self::assertSame(
            $component,
            self::getPrivatePropertyValue($dataLoader, 'component'),
        );
        self::assertSame(
            $configuration,
            self::getPrivatePropertyValue($dataLoader, 'jobConfiguration'),
        );
        self::assertSame(
            $state,
            self::getPrivatePropertyValue($dataLoader, 'jobState'),
        );
        self::assertSame(
            $logger,
            self::getPrivatePropertyValue($dataLoader, 'logger'),
        );
        self::assertSame(
            '/subpath',
            self::getPrivatePropertyValue($dataLoader, 'targetDataDirPath'),
        );
    }
}
