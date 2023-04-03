<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\State\Storage\Tables;

readonly class TablesList
{
    public function __construct(
        /** @var $items Table[] */
        private array $items,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            items: array_map(Table::fromArray(...), $data),
        );
    }

    public function toArray(): array
    {
        return array_map(
            fn(Table $table) => $table->toArray(),
            $this->items,
        );
    }
}
