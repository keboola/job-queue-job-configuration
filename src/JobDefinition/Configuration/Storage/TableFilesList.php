<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage;

use Countable;

readonly class TableFilesList implements Countable
{
    public function __construct(
        /** @var $items array[] */
        private array $items,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            items: $data,
        );
    }

    public function count(): int
    {
        return count($this->items);
    }

    public function isEmpty(): bool
    {
        return count($this->items) === 0;
    }

    public function toArray(): array
    {
        return $this->items;
    }
}
