<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State;

use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Keboola\JobQueue\JobConfiguration\Exception\InvalidDataException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Storage;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

readonly class State
{
    public function __construct(
        public Storage $storage = new Storage(),
        public array $component = [],
        public ?array $dataApp = null,
    ) {
    }

    public static function fromArray(array $data): self
    {
        try {
            $data = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $data]);
        } catch (InvalidConfigurationException $e) {
            throw new InvalidDataException(
                sprintf('Job state data is not valid: %s', $e->getMessage()),
                $data,
                $e,
            );
        }

        return new self(
            storage: Storage::fromArray($data['storage'] ?? []),
            component: $data['component'] ?? [],
            dataApp: $data['data_app'] ?? null,
        );
    }

    public function toArray(): array
    {
        $data = [
            'storage' => $this->storage->toArray(),
            'component' => $this->component,
        ];

        if ($this->dataApp !== null) {
            $data['data_app'] = $this->dataApp;
        }

        return $data;
    }

    public function withStorage(Storage $storage): self
    {
        return new self(
            storage: $storage,
            component: $this->component,
            dataApp: $this->dataApp,
        );
    }

    public function withComponent(array $component): self
    {
        return new self(
            storage: $this->storage,
            component: $component,
            dataApp: $this->dataApp,
        );
    }

    public function withDataApp(?array $dataApp): self
    {
        return new self(
            storage: $this->storage,
            component: $this->component,
            dataApp: $dataApp,
        );
    }
}
