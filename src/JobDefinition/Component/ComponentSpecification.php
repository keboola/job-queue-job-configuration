<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Component;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\JobQueue\JobConfiguration\Exception\ComponentInvalidException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Logging\GelfLoggingConfiguration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Logging\LoggingConfigurationInterface;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Logging\StandardLoggingConfiguration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\UnitConverter;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class ComponentSpecification
{
    private string $id;
    private array $data;
    private string $networkType;
    private array $features;

    /**
     * @param array $componentData Component data as returned by Storage API
     */
    public function __construct(array $componentData)
    {
        $this->id = empty($componentData['id']) ? '' : $componentData['id'];
        $data = empty($componentData['data']) ? [] : $componentData['data'];
        if (isset($componentData['features'])) {
            $this->features = $componentData['features'];
        } else {
            $this->features = [];
        }

        try {
            $this->data = (new Processor())->processConfiguration(
                new ComponentSpecificationDefinition(),
                ['component' => $data],
            );
        } catch (InvalidConfigurationException $e) {
            throw new ComponentInvalidException(
                'Component definition is invalid. Verify the deployment setup and the repository settings ' .
                'in the Developer Portal. Detail: ' . $e->getMessage(),
                $data,
                $e,
            );
        }

        $this->networkType = $this->data['network'];
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getSanitizedComponentId(): string
    {
        $result = preg_replace('/[^a-zA-Z0-9-]/i', '-', $this->getId());
        assert(is_string($result));

        return $result;
    }

    /**
     * @return 'json'|'yaml'
     */
    public function getConfigurationFormat(): string
    {
        return $this->data['configuration_format'];
    }

    public function getImageParameters(): array
    {
        return $this->data['image_parameters'];
    }

    public function hasDefaultBucket(): bool
    {
        return !empty($this->data['default_bucket']);
    }

    public function getDefaultBucketName(string $configId): string
    {
        return $this->data['default_bucket_stage'] . '.c-' . $this->getSanitizedComponentId() . '-' . $configId;
    }

    public function hasForwardToken(): bool
    {
        return (bool) $this->data['forward_token'];
    }

    public function hasForwardTokenDetails(): bool
    {
        return (bool) $this->data['forward_token_details'];
    }

    public function getType(): string
    {
        return $this->data['definition']['type'];
    }

    public function runAsRoot(): bool
    {
        return in_array('container-root-user', $this->features);
    }

    public function overrideKeepalive60s(): bool
    {
        return in_array('container-tcpkeepalive-60s-override', $this->features);
    }

    public function blockBranchJobs(): bool
    {
        return in_array('dev-branch-job-blocked', $this->features);
    }

    public function branchConfigurationsAreUnsafe(): bool
    {
        return in_array('dev-branch-configuration-unsafe', $this->features);
    }

    public function allowBranchMapping(): bool
    {
        return in_array('dev-mapping-allowed', $this->features);
    }

    public function hasNoSwap(): bool
    {
        return in_array('no-swap', $this->features);
    }

    public function allowUseFileStorageOnly(): bool
    {
        return in_array('allow-use-file-storage-only', $this->features);
    }

    public function allowMlflowArtifactsAccess(): bool
    {
        return in_array('mlflow-artifacts-access', $this->features, true);
    }

    public function getLoggerType(): string
    {
        if (!empty($this->data['logging'])) {
            return $this->data['logging']['type'];
        }
        return 'standard';
    }

    public function getLoggerVerbosity(): array
    {
        if (!empty($this->data['logging'])) {
            return $this->data['logging']['verbosity'];
        }
        return [];
    }

    public function getLoggingConfiguration(): LoggingConfigurationInterface
    {
        $logging = $this->definition['data']['logging'] ?? [];

        return match ($logging['type'] ?? 'standard') {
            'standard' => StandardLoggingConfiguration::fromArray($logging),
            'gelf' => GelfLoggingConfiguration::fromArray($logging),
            default => throw new ComponentInvalidException(sprintf(
                'Invalid logging type "%s". Valid values are "standard" or "gelf"',
                $logging['type'],
            )),
        };
    }

    public function getNetworkType(): string
    {
        return $this->networkType;
    }

    public function getMemoryLimit(): string
    {
        return $this->data['memory'];
    }

    public function getMemoryLimitBytes(): int
    {
        return UnitConverter::connectionMemoryLimitToBytes($this->getMemoryLimit());
    }

    public function getProcessTimeout(): int
    {
        return (int) ($this->data['process_timeout']);
    }

    public function getImageDefinition(): array
    {
        return $this->data['definition'];
    }

    public function setImageTag(string $tag): void
    {
        $this->data['definition']['tag'] = $tag;
    }

    public function getImageUri(): string
    {
        return $this->data['definition']['uri'];
    }

    public function getImageTag(): string
    {
        return $this->data['definition']['tag'];
    }

    public function getImageUriWithTag(?string $customTag): string
    {
        return sprintf('%s:%s', $this->getImageUri(), $customTag ?? $this->getImageTag());
    }

    public function getSynchronousActions(): array
    {
        return $this->data['synchronous_actions'] ?? [];
    }

    public function hasSynchronousAction(string $action): bool
    {
        return in_array($action, $this->getSynchronousActions(), true);
    }

    public function getStagingStorage(): array
    {
        return $this->data['staging_storage'];
    }

    /**
     * @return non-empty-string
     */
    public function getInputStagingStorage(): string
    {
        return $this->getStagingStorage()['input'] ?? AbstractStrategyFactory::LOCAL;
    }

    /**
     * @return non-empty-string
     */
    public function getOutputStagingStorage(): string
    {
        return $this->getStagingStorage()['output'] ?? AbstractStrategyFactory::LOCAL;
    }

    public function isApplicationErrorDisabled(): bool
    {
        return (bool) $this->data['logging']['no_application_errors'];
    }
}
