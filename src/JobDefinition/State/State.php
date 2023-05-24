<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State;

use Keboola\JobQueue\JobConfiguration\Exception\ApplicationException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Storage;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

readonly class State
{
    public function __construct(
        public Storage $storage = new Storage(),
        public array $component = [],
    ) {
    }

    public static function fromArray(array $data): self
    {
        try {
            $data = (new Processor())->processConfiguration(new StateDefinition(), ['state' => $data]);
        } catch (InvalidConfigurationException $e) {
            throw new ApplicationException(sprintf('Job state is not valid: %s', $e->getMessage()), $data, $e);
        }

        return new self(
            storage: Storage::fromArray($data['storage'] ?? []),
            component: $data['component'] ?? [],
        );
    }

    public function toArray(): array
    {
        return [
            'storage' => $this->storage->toArray(),
            'component' => $this->component,
        ];
    }
}
