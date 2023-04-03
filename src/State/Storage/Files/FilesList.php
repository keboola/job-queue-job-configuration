<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\State\Storage\Files;

readonly class FilesList
{
    public function __construct(
        /** @var $items File[] */
        private array $items,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            items: array_map(File::fromArray(...), $data),
        );
    }

    public function toArray(): array
    {
        return array_map(
            fn(File $file) => $file->toArray(),
            $this->items,
        );
    }
}
