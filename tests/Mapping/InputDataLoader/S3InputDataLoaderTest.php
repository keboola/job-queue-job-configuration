<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\Csv\CsvFile;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Symfony\Component\Filesystem\Filesystem;

class S3InputDataLoaderTest extends BaseInputDataLoaderTestCase
{
    protected const RESOURCE_SUFFIX = '-s3';

    public function setUp(): void
    {
        parent::setUp();
    }

    private function getS3StagingComponent(): ComponentSpecification
    {
        return new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'staging-storage' => [
                    'input' => 's3',
                ],
            ],
        ]);
    }

    public function testStoreArchive(): void
    {
        $this->markTestSkipped('Will be implemented in separate PR, see Jira issue PST-2182');
    }

    public function testLoadInputDataS3(): void
    {
        $bucketId = $this->clientWrapper->getBasicClient()->createBucket($this->getResourceName(), 'in');

        $storage = new Storage(
            input: new Input(
                tables: new TablesList([
                    [
                        'source' => "$bucketId.test",
                    ],
                ]),
            ),
        );
        $fs = new Filesystem();
        $filePath = $this->getDataDirPath() . '/in/tables/test.csv';
        $fs->dumpFile(
            $filePath,
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );
        $this->clientWrapper->getBasicClient()->createTable(
            $bucketId,
            'test',
            new CsvFile($filePath),
        );

        $component = $this->getS3StagingComponent();
        $dataLoader = $this->getInputDataLoader($component, $this->clientWrapper);
        $dataLoader->loadInputData(
            component: $component,
            jobConfiguration: new Configuration(
                storage: $storage,
            ),
            jobState: new State(),
        );

        $manifest = json_decode(
            // @phpstan-ignore-next-line
            file_get_contents(
                $this->getDataDirPath() . "/in/tables/$bucketId.test.manifest",
            ),
            true,
        );

        self::assertIsArray($manifest);
        $this->assertS3info($manifest);
    }

    private function assertS3info(array $manifest): void
    {
        self::assertArrayHasKey('s3', $manifest);
        self::assertArrayHasKey('isSliced', $manifest['s3']);
        self::assertArrayHasKey('region', $manifest['s3']);
        self::assertArrayHasKey('bucket', $manifest['s3']);
        self::assertArrayHasKey('key', $manifest['s3']);
        self::assertArrayHasKey('credentials', $manifest['s3']);
        self::assertArrayHasKey('access_key_id', $manifest['s3']['credentials']);
        self::assertArrayHasKey('secret_access_key', $manifest['s3']['credentials']);
        self::assertArrayHasKey('session_token', $manifest['s3']['credentials']);
        self::assertStringContainsString('.gz', $manifest['s3']['key']);
        if ($manifest['s3']['isSliced']) {
            self::assertStringContainsString('manifest', $manifest['s3']['key']);
        }
    }
}
