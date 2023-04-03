<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Configuration\Storage;

readonly class Output
{
    public function __construct(
        public TablesList $tables,
        public FilesList $files,
        public TableFilesList $tableFiles,
        public ?string $defaultBucket,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            tables: TablesList::fromArray($data['tables'] ?? []),
            files: FilesList::fromArray($data['files'] ?? []),
            tableFiles: TableFilesList::fromArray($data['table_files'] ?? []),
            defaultBucket: isset($data['default_bucket']) ? (string) $data['default_bucket'] : null,
        );
    }

    public function toArray(): array
    {
        return [
            'tables' => $this->tables->toArray(),
            'files' => $this->files->toArray(),
            'table_files' => $this->tableFiles->toArray(),
            'default_bucket' => $this->defaultBucket,
        ];
    }

    public function isEmpty(): bool
    {
        return $this->tables->isEmpty() && $this->files->isEmpty() && $this->tableFiles->isEmpty();
    }
}
