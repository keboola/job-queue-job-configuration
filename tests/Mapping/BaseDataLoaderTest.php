<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

abstract class BaseDataLoaderTest extends TestCase
{
    private const DEFAULT_COMPONENT_STAGING_STORAGE_TYPE = 'local';
    protected const COMPONENT_ID = 'docker-demo';

    private string $workingDirPath;
    protected ClientWrapper $clientWrapper;

    protected function setUp(): void
    {
        parent::setUp();

        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('TEST_STORAGE_API_TOKEN'),
            ),
        );

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

    protected function getTmpDirPath(): string
    {
        return $this->workingDirPath . '/tmp';
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

    protected function getComponentWithDefaultBucket(
        ?string $stagingStorageType = null,
    ): ComponentSpecification {
        return new ComponentSpecification([
            'id' => static::COMPONENT_ID,
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'default_bucket' => true,
                'staging_storage' => [
                    'input' => $stagingStorageType ?? self::DEFAULT_COMPONENT_STAGING_STORAGE_TYPE,
                    'output' => $stagingStorageType ?? self::DEFAULT_COMPONENT_STAGING_STORAGE_TYPE,
                ],
            ],
        ]);
    }

    protected function getComponent(): ComponentSpecification
    {
        return new ComponentSpecification([
            'id' => static::COMPONENT_ID,
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
        $files = $this->clientWrapper->getBasicClient()->listFiles(
            (new ListFilesOptions())->setTags(['docker-demo-test' . $suffix]),
        );
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file['id']);
        }

        $bucketId = self::getBucketIdByDisplayName($this->clientWrapper, 'docker-demo-testConfig' . $suffix, 'in');

        if ($bucketId === null) {
            return;
        }

        try {
            $this->clientWrapper->getBasicClient()->dropBucket(
                $bucketId,
                ['force' => true, 'async' => true],
            );
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
    }

    protected static function getBucketIdByDisplayName(
        ClientWrapper $clientWrapper,
        string $bucketDisplayName,
        string $stage
    ): ?string {
        $buckets = $clientWrapper->getBasicClient()->listBuckets();
        foreach ($buckets as $bucket) {
            if ($bucket['displayName'] === $bucketDisplayName && $bucket['stage'] === $stage) {
                return $bucket['id'];
            }
        }
        return null;
    }
}
