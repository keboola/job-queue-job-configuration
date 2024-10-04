<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\InputDataLoader;

use Keboola\Csv\CsvFile;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\FilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Input;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\Storage;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TablesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use Keboola\StorageApi\Options\FileUploadOptions;
use SplFileInfo;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class GCSInputDataLoaderTest extends BaseInputDataLoaderTest
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanupBucketAndFiles('-gcs');
    }

    public function testLoadInputData(): void
    {
        $storage = new Storage(
            input: new Input(
                tables: new TablesList([
                    [
                        'source' => 'in.c-docker-demo-testConfig-gcs.test',
                    ],
                ]),
                files: new FilesList([
                    ['tags' => ['docker-demo-test-gcs'], 'overwrite' => true],
                ]),
            ),
        );
        $fs = new Filesystem();
        $filePath = $this->getDataDirPath() . '/upload/tables/test.csv';
        $fs->dumpFile(
            $filePath,
            "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
        );

        $this->clientWrapper->getBasicClient()->createBucket('docker-demo-testConfig-gcs', 'in');
        $this->clientWrapper->getBasicClient()->createTable(
            'in.c-docker-demo-testConfig-gcs',
            'test',
            new CsvFile($filePath),
        );
        $this->clientWrapper->getBasicClient()->uploadFile(
            $filePath,
            (new FileUploadOptions())->setTags(['docker-demo-test-gcs']),
        );
        sleep(1);

        $dataLoader = $this->getInputDataLoader();
        $dataLoader->loadInputData(
            component: $this->getComponent(),
            jobConfiguration: new Configuration(
                storage: $storage,
            ),
            jobState: new State(),
        );

        $manifest = json_decode(
            // @phpstan-ignore-next-line
            file_get_contents(
                $this->getDataDirPath() . '/in/tables/in.c-docker-demo-testConfig-gcs.test.manifest',
            ),
            true,
        );

        $finder = new Finder();
        $finder->files()->in($this->getDataDirPath() . '/in/files')->notName('*.manifest');

        $this->assertEquals(1, $finder->count());

        /** @var SplFileInfo $file */
        foreach ($finder as $file) {
            $this->assertEquals(
                "id,text,row_number\n1,test,1\n1,test,2\n1,test,3",
                file_get_contents($file->getPathname()),
            );

            $fileManifest = json_decode(
                // @phpstan-ignore-next-line
                file_get_contents($file->getPathname() . '.manifest'),
                true,
            );

            self::assertIsArray($fileManifest);
            self::assertArrayHasKey('id', $fileManifest);
            self::assertArrayHasKey('name', $fileManifest);
            self::assertArrayHasKey('created', $fileManifest);
            self::assertArrayHasKey('is_public', $fileManifest);
            self::assertArrayHasKey('is_encrypted', $fileManifest);
            self::assertArrayHasKey('tags', $fileManifest);
            self::assertArrayHasKey('max_age_days', $fileManifest);
            self::assertArrayHasKey('size_bytes', $fileManifest);
            self::assertArrayHasKey('is_sliced', $fileManifest);
        }

        $this->assertIsArray($manifest);
        $this->assertArrayHasKey('id', $manifest);
        $this->assertArrayHasKey('name', $manifest);
        $this->assertArrayHasKey('created', $manifest);
        $this->assertArrayHasKey('uri', $manifest);
        $this->assertArrayHasKey('primary_key', $manifest);
        $this->assertEquals('in.c-docker-demo-testConfig-gcs.test', $manifest['id']);
        $this->assertEquals('test', $manifest['name']);
    }
}
