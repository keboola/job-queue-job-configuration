<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FilesList;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Tables\TablesList;

readonly class Input
{
    public function __construct(
        public TablesList $tables = new TablesList(),
        public FilesList $files = new FilesList(),
    ) {
    }

    public static function fromArray(array $data): self
    {
        /** @var array $tables */
        $tables = $data['tables'] ?? [];
        /** @var array $files */
        $files = $data['files'] ?? [];

        return new self(
            tables: TablesList::fromArray($tables),
            files: FilesList::fromArray($files),
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
