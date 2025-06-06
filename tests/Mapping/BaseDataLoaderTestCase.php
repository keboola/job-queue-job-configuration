<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping;

use InvalidArgumentException;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\Tests\BackendAssertsTrait;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\Attribute\UseAzureProject;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\Attribute\UseGCPProject;
use Keboola\JobQueue\JobConfiguration\Tests\Mapping\Attribute\UseSnowflakeProject;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ReflectionClass;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;

abstract class BaseDataLoaderTestCase extends TestCase
{
    use BackendAssertsTrait;

    private const DEFAULT_COMPONENT_STAGING_STORAGE_TYPE = 'local';
    protected const COMPONENT_ID = 'docker-demo';
    protected const DEFAULT_PROJECT = 'snowflake';

    /** @var non-empty-string */
    private string $workingDirPath;
    protected ClientWrapper $clientWrapper;

    abstract protected static function expectedDefaultTableBackend(): string;

    protected function setUp(): void
    {
        parent::setUp();

        $this->initClientWrapper();
        $this->assertDefaultTableBackend(static::expectedDefaultTableBackend(), $this->clientWrapper->getBasicClient());
        $this->prepareWorkingDir();
        $this->cleanupBucketAndFiles();
    }

    /**
     * @return non-empty-string
     */
    protected function getWorkingDirPath(): string
    {
        return $this->workingDirPath;
    }

    /**
     * @return non-empty-string
     */
    protected function getDataDirPath(): string
    {
        return $this->workingDirPath . '/data';
    }

    /**
     * @return non-empty-string
     */
    protected function getTmpDirPath(): string
    {
        return $this->workingDirPath . '/tmp';
    }

    private function prepareWorkingDir(): void
    {
        $temp = new Temp();
        $fs = new Filesystem();

        $workingDirPath = $temp->getTmpFolder();
        assert($workingDirPath !== '');

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

    protected function getComponent(
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
                'staging_storage' => [
                    'input' => $stagingStorageType ?? self::DEFAULT_COMPONENT_STAGING_STORAGE_TYPE,
                    'output' => $stagingStorageType ?? self::DEFAULT_COMPONENT_STAGING_STORAGE_TYPE,
                ],
            ],
        ]);
    }

    protected function cleanupBucketAndFiles(): void
    {
        $files = $this->clientWrapper->getBasicClient()->listFiles(
            (new ListFilesOptions())->setTags([$this->getResourceName()]),
        );
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file['id']);
        }

        $this->cleanupBucket();
    }

    protected function cleanupBucket(): void
    {
        $bucketId = self::getBucketIdByDisplayName(
            clientWrapper: $this->clientWrapper,
            bucketDisplayName: $this->getResourceName(),
            stage: 'in',
        );

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
        string $stage,
    ): ?string {
        $buckets = $clientWrapper->getBasicClient()->listBuckets();
        foreach ($buckets as $bucket) {
            if ($bucket['displayName'] === $bucketDisplayName && $bucket['stage'] === $stage) {
                return $bucket['id'];
            }
        }
        return null;
    }

    /**
     * Drop existing component's default bucket with the same name and create a new one.
     * @return string Bucket ID
     */
    protected static function dropAndCreateDefaultBucket(
        ClientWrapper $clientWrapper,
        string $stage,
        ComponentSpecification $component,
        string $configId,
    ): string {
        return self::dropAndCreateBucket(
            $clientWrapper,
            $component->getDefaultBucketName($configId),
            $stage,
        );
    }

    /**
     * Drop existing bucket with the same name and create a new one.
     * @return string Bucket ID
     */
    protected static function dropAndCreateBucket(
        ClientWrapper $clientWrapper,
        string $bucketDisplayName,
        string $stage,
    ): string {
        $storageApiClient = $clientWrapper->getBasicClient();

        foreach ($storageApiClient->listBuckets() as $bucket) {
            if ($bucket['displayName'] === $bucketDisplayName && $bucket['stage'] === $stage) {
                $storageApiClient->dropBucket($bucket['id'], ['async' => true, 'force' => true]);
            }
        }

        return $storageApiClient->createBucket($bucketDisplayName, $stage, static::class);
    }

    /**
     * Drop existing bucket with the same name.
     */
    protected static function dropBucket(ClientWrapper $clientWrapper, string $bucketId): void
    {
        $storageApiClient = $clientWrapper->getBasicClient();

        try {
            $storageApiClient->dropBucket($bucketId, ['async' => true, 'force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() === 404) {
                return;
            }

            throw $e;
        }
    }

    /**
     * Drop existing component's default bucket with the same name.
     */
    protected static function dropDefaultBucket(
        ClientWrapper $clientWrapper,
        ComponentSpecification $component,
        string $configId,
    ): void {
        self::dropBucket($clientWrapper, $component->getDefaultBucketName($configId));
    }

    private function getClientWrapperForGCPProject(bool $useMasterToken = false): ClientWrapper
    {
        $token = $useMasterToken
            ? getenv('TEST_STORAGE_API_TOKEN_MASTER_GCP')
            : getenv('TEST_STORAGE_API_TOKEN_GCP');

        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL_GCP'),
                (string) $token,
            ),
        );
    }

    private function getClientWrapperForAzureProject(bool $useMasterToken = false): ClientWrapper
    {
        $token = $useMasterToken
            ? getenv('TEST_STORAGE_API_TOKEN_MASTER_AZURE')
            : getenv('TEST_STORAGE_API_TOKEN_AZURE');

        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL_AZURE'),
                (string) $token,
            ),
        );
    }

    private function getClientWrapperForSnowflakeProject(
        bool $useMasterToken = false,
        ?string $nativeTypes = null,
    ): ClientWrapper {
        if ($useMasterToken && $nativeTypes) {
            throw new RuntimeException('Native types are not supported with master token!');
        }

        $token = match ($nativeTypes) {
            'new-native-types' => getenv('TEST_STORAGE_API_TOKEN_FEATURE_NEW_NATIVE_TYPES'),
            'native-types' => getenv('TEST_STORAGE_API_TOKEN_FEATURE_NATIVE_TYPES'),
            default => $useMasterToken
                ? getenv('TEST_STORAGE_API_TOKEN_MASTER')
                : getenv('TEST_STORAGE_API_TOKEN'),
        };

        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) $token,
            ),
        );
    }

    protected function getResourceName(): string
    {
        $name = $this->name();
        if ($this->dataName() !== '') {
            $dataName = preg_replace('/\s+/', '_', (string) $this->dataName());
            $name .= '-' . $dataName;
        }
        return $name;
    }

    private function initClientWrapper(): void
    {
        $reflection = new ReflectionClass($this);
        $method = $reflection->getMethod($this->name());
        $attributes = $method->getAttributes();

        $this->clientWrapper = match (static::DEFAULT_PROJECT) {
            'gcp' => $this->getClientWrapperForGCPProject(),
            'azure' => $this->getClientWrapperForAzureProject(),
            default => $this->getClientWrapperForSnowflakeProject(),
        };

        foreach ($attributes as $attribute) {
            $attributeInstance = $attribute->newInstance();

            $this->clientWrapper = match ($attributeInstance::class) {
                UseAzureProject::class => $this->getClientWrapperForAzureProject(
                    $attributeInstance->useMasterToken,
                ),
                UseGCPProject::class => $this->getClientWrapperForGCPProject(
                    $attributeInstance->useMasterToken,
                ),
                UseSnowflakeProject::class => $this->getClientWrapperForSnowflakeProject(
                    $attributeInstance->useMasterToken,
                    $attributeInstance->nativeTypes,
                ),
                default => throw new InvalidArgumentException('You have passed a not-implemented attribute.'),
            };
        }
    }
}
