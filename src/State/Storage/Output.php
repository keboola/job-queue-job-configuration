<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\State\Storage;

use Keboola\JobQueue\JobConfiguration\State\Storage\Files\FilesList;
use Keboola\JobQueue\JobConfiguration\State\Storage\Tables\TablesList;

readonly class Output
{
    public function __construct(
        public TablesList $tables,
        public FilesList $files,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            tables: TablesList::fromArray($data['tables'] ?? []),
            files: FilesList::fromArray($data['files'] ?? []),
        );
    }

    public function toArray(): array
    {
        return [
            'tables' => $this->tables->toArray(),
            'files' => $this->files->toArray(),
        ];
    }
}
