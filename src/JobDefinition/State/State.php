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
        public Storage $storage,
        public ?array $component,
    ) {
    }

    public static function fromArray(array $data): self
    {
        try {
            $data = (new Processor())->processConfiguration(new StateSpec(), ['state' => $data]);
        } catch (InvalidConfigurationException $e) {
            throw new ApplicationException(sprintf('Job state is not valid: %s', $e->getMessage()), $data, $e);
        }

        return new self(
            storage: Storage::fromArray($data['storage'] ?? []),
            component: $data['component'] ?? null,
        );
    }

    public function toArray(): array
    {
        $data = [
            'storage' => $this->storage->toArray(),
        ];

        if ($this->component !== null) {
            $data['component'] = $this->component;
        }

        return $data;
    }
}
