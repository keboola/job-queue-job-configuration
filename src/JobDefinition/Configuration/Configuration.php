<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration;

use Keboola\JobQueue\JobConfiguration\Exception\InvalidDataException;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

readonly class Configuration
{
    public function __construct(
        public array $parameters = [],
        public Storage\Storage $storage = new Storage\Storage(),
        public Processors\Processors $processors = new Processors\Processors(),
        public ?Runtime\Runtime $runtime = null,
        public ?string $variablesId = null,
        public ?string $variablesValuesId = null,
        public ?string $sharedCodeId = null,
        /** @var string[] */
        public array $sharedCodeRowIds = [],
        public ?array $imageParameters = null,
        public array $authorization = [],
        public ?string $action = null,
        public Artifacts\Artifacts $artifacts = new Artifacts\Artifacts(),
    ) {
    }

    public static function fromArray(array $data): self
    {
        try {
            $data = (new Processor())->processConfiguration(new ConfigurationDefinition(), ['configuration' => $data]);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidDataException(
                sprintf('Job configuration data is not valid: %s', $e->getMessage()),
                $data,
                $e,
            );
        }

        return new self(
            parameters: $data['parameters'] ?? [],
            storage: Storage\Storage::fromArray($data['storage'] ?? []),
            processors: Processors\Processors::fromArray($data['processors'] ?? []),
            runtime: isset($data['runtime']) ? Runtime\Runtime::fromArray($data['runtime']) : null,
            variablesId: isset($data['variables_id']) ? (string) $data['variables_id'] : null,
            variablesValuesId: isset($data['variables_values_id']) ? (string) $data['variables_values_id'] : null,
            sharedCodeId: isset($data['shared_code_id']) ? (string) $data['shared_code_id'] : null,
            sharedCodeRowIds: array_map(strval(...), $data['shared_code_row_ids'] ?? []),
            imageParameters: $data['image_parameters'] ?? null,
            authorization: $data['authorization'] ?? [],
            action: $data['action'] ?? null,
            artifacts: Artifacts\Artifacts::fromArray($data['artifacts'] ?? []),
        );
    }

    public function toArray(): array
    {
        $data = [
            'action' => $this->action,
            'parameters' => $this->parameters,
            'storage' => $this->storage->toArray(),
            'processors' => $this->processors->toArray(),
            'artifacts' => $this->artifacts->toArray(),
        ];

        if ($this->runtime !== null) {
            $data['runtime'] = $this->runtime->toArray();
        }

        if ($this->variablesId !== null) {
            $data['variables_id'] = $this->variablesId;
        }

        if ($this->variablesValuesId !== null) {
            $data['variables_values_id'] = $this->variablesValuesId;
        }

        if ($this->sharedCodeId !== null) {
            $data['shared_code_id'] = $this->sharedCodeId;
            $data['shared_code_row_ids'] = $this->sharedCodeRowIds;
        }

        if ($this->imageParameters !== null && $this->imageParameters !== []) {
            $data['image_parameters'] = $this->imageParameters;
        }

        if ($this->authorization !== null && $this->authorization !== []) {
            $data['authorization'] = $this->authorization;
        }

        return $data;
    }

    public function mergeArray(array $data): self
    {
        return self::fromArray(array_replace_recursive($this->toArray(), $data));
    }
}
