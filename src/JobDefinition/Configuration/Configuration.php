<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration;

use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

readonly class Configuration
{
    public function __construct(
        public array $parameters,
        public Storage\Storage $storage,
        public array $processors = [],
        public ?Runtime\Runtime $runtime = null,
    ) {
    }

    public static function fromArray(array $data): self
    {
        try {
            $data = (new Processor())->processConfiguration(new ConfigurationSpec(), ['configuration' => $data]);
        } catch (InvalidConfigurationException $e) {
            throw new ApplicationException(sprintf('Job configuration is not valid: %s', $e->getMessage()), $data, $e);
        }

        return new self(
            parameters: $data['parameters'] ?? [],
            storage: Storage\Storage::fromArray($data['storage'] ?? []),
            processors: $data['processors'] ?? [],
            runtime: isset($data['runtime']) ? Runtime\Runtime::fromArray($data['runtime']) : null,
        );
    }

    public function toArray(): array
    {
        $data = [
            'parameters' => $this->parameters,
            'storage' => $this->storage->toArray(),
            'processors' => $this->processors,
        ];

        if ($this->runtime) {
            $data['runtime'] = $this->runtime->toArray();
        }

        return $data;
    }

    public function mergeArray(array $data): self
    {
        return self::fromArray(array_replace_recursive($this->toArray(), $data));
    }
}
