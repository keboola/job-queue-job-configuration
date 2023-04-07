<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\TablesList;

readonly class Input
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
