<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\State\Storage\Tables;

readonly class Table
{
    public function __construct(
        public string $source,
        public string $lastImportDate,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            source: (string) $data['source'],
            lastImportDate: (string) $data['lastImportDate'],
        );
    }

    public function toArray(): array
    {
        return [
            'source' => $this->source,
            'lastImportDate' => $this->lastImportDate,
        ];
    }
}
