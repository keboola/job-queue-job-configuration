<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

abstract class BaseDataLoaderTest extends TestCase
{
    private string $workingDirPath;
    protected ClientWrapper $clientWrapper;

    protected function setUp(): void
    {
        parent::setUp();

        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN'), // todo: change to STORAGE_API_TOKEN?
            ),
        );
        $this->metadata = new Metadata($this->clientWrapper->getBasicClient());

        $this->prepareWorkingDir();
    }

    protected function getWorkingDirPath(): string
    {
        return $this->workingDirPath;
    }

    protected function getDataDirPath(): string
    {
        return $this->workingDirPath . '/data';
    }

    private function prepareWorkingDir(): void
    {
        $temp = new Temp();
        $fs = new Filesystem();

        $workingDirPath = $temp->getTmpFolder();

        $fs->mkdir([
            $workingDirPath,
            $workingDirPath . '/tmp',
            $workingDirPath . '/data',
            $workingDirPath . '/data/in',
            $workingDirPath . '/data/in/tables',
            $workingDirPath . '/data/in/files',
            $workingDirPath . '/data/in/user',
            $workingDirPath . '/data/out',
            $workingDirPath . '/data/out/tables',
            $workingDirPath . '/data/out/files',
        ]);

        $this->workingDirPath = $workingDirPath;
    }

    protected function getComponentWithDefaultBucket(): ComponentSpecification
    {
        return new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'default_bucket' => true,
            ],
        ]);
    }

    protected function getComponent(): ComponentSpecification
    {
        return new ComponentSpecification([
            'id' => 'docker-demo',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
            ],
        ]);
    }

    protected function cleanupBucketAndFiles($suffix = ''): void
    {
        try {
            $this->clientWrapper->getBasicClient()->dropBucket(
                'in.c-docker-demo-testConfig' . $suffix,
                ['force' => true, 'async' => true],
            );
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $files = $this->clientWrapper->getBasicClient()->listFiles(
            (new ListFilesOptions())->setTags(['docker-demo-test' . $suffix]),
        );
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file['id']);
        }
    }
}