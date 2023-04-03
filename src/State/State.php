<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\State;

use Keboola\JobQueue\JobConfiguration\State\Storage\Storage;

readonly class State
{
    public function __construct(
        public Storage $storage,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            storage: Storage::fromArray($data['storage'] ?? []),
        );
    }

    public function toArray(): array
    {
        return [
            'storage' => $this->storage->toArray(),
        ];
    }
}
