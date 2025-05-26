<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping\StagingWorkspace;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Backend;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Runtime;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials;
use Keboola\JobQueue\JobConfiguration\Mapping\StagingWorkspace\StagingWorkspaceFactory;
use Keboola\JobQueue\JobConfiguration\Tests\TestEnvVarsTrait;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\StagingProvider\Workspace\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration as StorageApiConfig;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class StagingWorkspaceFactoryFunctionalTest extends TestCase
{
    use TestEnvVarsTrait;

    private readonly ClientWrapper $clientWrapper;
    private readonly Workspaces $workspacesApiClient;
    private readonly Components $componentsApiClient;

    private array $createdWorkspaceIds = [];
    /** @var StorageApiConfig[] */
    private array $createdConfigurations = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                self::getRequiredEnv('STORAGE_API_URL'),
                self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            ),
        );
        $this->workspacesApiClient = new Workspaces($this->clientWrapper->getBranchClient());
        $this->componentsApiClient = new Components($this->clientWrapper->getBranchClient());
    }

    protected function tearDown(): void
    {
        foreach ($this->createdWorkspaceIds as $workspaceId) {
            try {
                $this->workspacesApiClient->deleteWorkspace((int) $workspaceId, async: true);
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
        }

        foreach ($this->createdConfigurations as $configuration) {
            try {
                $this->componentsApiClient->deleteConfiguration(
                    $configuration->getComponentId(),
                    $configuration->getConfigurationId(),
                );

                // double-delete to remove from trash
                $this->componentsApiClient->deleteConfiguration(
                    $configuration->getComponentId(),
                    $configuration->getConfigurationId(),
                );
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
        }

        parent::tearDown();
    }

    private function createWorkspace(array $options): array
    {
        $workspaceData = $this->workspacesApiClient->createWorkspace($options, async: true);
        $this->createdWorkspaceIds[] = (string) $workspaceData['id'];
        return $workspaceData;
    }

    private function createConfiguration(): StorageApiConfig
    {
        $componentId = 'keboola.runner-staging-test';
        $configId = 'staging-workspace-factory-functional-test-' . uniqid();

        $configuration = new StorageApiConfig();
        $configuration->setConfigurationId($configId);
        $configuration->setName('Staging Workspace Factory Functional Test');
        $configuration->setComponentId($componentId);
        $this->componentsApiClient->addConfiguration($configuration);

        $this->createdConfigurations[] = $configuration;

        return $configuration;
    }

    private function workspaceExists(string $workspaceId): bool
    {
        $workspacesApiClient = new Workspaces($this->clientWrapper->getBranchClient());
        try {
            $workspacesApiClient->getWorkspace($workspaceId);
            return true;
        } catch (ClientException $e) {
            if ($e->getCode() === 404) {
                return false;
            }
            throw $e;
        }
    }

    public function testExistingWorkspaceWithExternalCredentials(): void
    {
        if ($this->clientWrapper->getToken()->getProjectBackend() !== 'snowflake') {
            // each backend has specific credentials, so we test just Snowflake
            self::markTestSkipped('Test only for snowflake backend');
        }

        $snowflakeKeypairGenerator = new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator());
        $keyPair = $snowflakeKeypairGenerator->generateKeyPair();

        $workspacesApiClient = new Workspaces($this->clientWrapper->getBranchClient());
        $workspace = $this->createWorkspace([
            'backend' => 'snowflake',
            'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            'publicKey' => $keyPair->publicKey,
        ]);

        $configuration = new Configuration(
            runtime: new Runtime(
                backend: new Backend(
                    workspaceCredentials: WorkspaceCredentials::fromArray([
                        'id' => $workspace['id'],
                        'type' => $workspace['connection']['backend'],
                        '#privateKey' => $keyPair->privateKey,
                    ]),
                ),
            ),
        );

        $component = new ComponentSpecification([
            'id' => 'test-component',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'test/test',
                ],
                'staging-storage' => [
                    'input' => 'workspace-snowflake',
                    'output' => 'workspace-snowflake',
                ],
            ],
        ]);

        $factory = new StagingWorkspaceFactory(
            new WorkspaceProvider(
                new Workspaces($this->clientWrapper->getBranchClient()),
                new Components($this->clientWrapper->getBranchClient()),
                $snowflakeKeypairGenerator,
            ),
            new NullLogger(),
        );

        // create the workspace facade
        $stagingWorkspaceFacade = $factory->createStagingWorkspaceFacade(
            $this->clientWrapper->getToken(),
            $component,
            $configuration,
            null,
        );

        self::assertNotNull($stagingWorkspaceFacade);
        self::assertSame((string) $workspace['id'], $stagingWorkspaceFacade->getWorkspaceId());

        // cleanup workspace (should not be deleted)
        $stagingWorkspaceFacade->cleanup();
        self::assertTrue($this->workspaceExists((string) $workspace['id']));
    }

    public function testNewWorkspaceCreation(): void
    {
        $stagingType = 'workspace-' . $this->clientWrapper->getToken()->getProjectBackend();
        $component = new ComponentSpecification([
            'id' => 'test-component',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'test/test',
                ],
                'staging-storage' => [
                    'input' => $stagingType,
                    'output' => $stagingType,
                ],
            ],
        ]);

        $factory = new StagingWorkspaceFactory(
            new WorkspaceProvider(
                new Workspaces($this->clientWrapper->getBranchClient()),
                new Components($this->clientWrapper->getBranchClient()),
                new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator()),
            ),
            new NullLogger(),
        );

        // create the workspace facade
        $stagingWorkspaceFacade = $factory->createStagingWorkspaceFacade(
            $this->clientWrapper->getToken(),
            $component,
            new Configuration(),
            null,
        );

        self::assertNotNull($stagingWorkspaceFacade);
        $workspaceId = $stagingWorkspaceFacade->getWorkspaceId();

        self::assertNotNull($workspaceId);
        self::assertTrue($this->workspaceExists($workspaceId));

        // cleanup
        $stagingWorkspaceFacade->cleanup();

        // verify the workspace no longer exists after cleanup
        self::assertFalse($this->workspaceExists($workspaceId));
    }

    public function testWorkspaceCreationWithConfigId(): void
    {
        $config = $this->createConfiguration();

        $stagingType = 'workspace-' . $this->clientWrapper->getToken()->getProjectBackend();
        $component = new ComponentSpecification([
            'id' => $config->getComponentId(),
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'test/test',
                ],
                'staging-storage' => [
                    'input' => $stagingType,
                    'output' => $stagingType,
                ],
            ],
        ]);

        $configId = $config->getConfigurationId();
        assert(is_string($configId)); // getConfigurationId has an invalid return type

        $factory = new StagingWorkspaceFactory(
            new WorkspaceProvider(
                new Workspaces($this->clientWrapper->getBranchClient()),
                new Components($this->clientWrapper->getBranchClient()),
                new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator()),
            ),
            new NullLogger(),
        );

        // create the workspace facade with the real configId
        $stagingWorkspaceFacade = $factory->createStagingWorkspaceFacade(
            $this->clientWrapper->getToken(),
            $component,
            new Configuration(),
            $configId,
        );

        self::assertNotNull($stagingWorkspaceFacade);
        $workspaceId = $stagingWorkspaceFacade->getWorkspaceId();

        self::assertNotNull($workspaceId);
        self::assertTrue($this->workspaceExists($workspaceId));

        // Get the workspace details to verify configId was used
        $workspacesApiClient = new Workspaces($this->clientWrapper->getBranchClient());
        $workspace = $workspacesApiClient->getWorkspace($workspaceId);

        self::assertSame($config->getConfigurationId(), $workspace['configurationId']);
        self::assertSame($config->getComponentId(), $workspace['component']);

        // cleanup workspace
        $stagingWorkspaceFacade->cleanup();

        // verify the workspace no longer exists after cleanup
        self::assertFalse($this->workspaceExists($workspaceId));
    }
}
