<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage;

readonly class Output
{
    public function __construct(
        public TablesList $tables = new TablesList(),
        public FilesList $files = new FilesList(),
        public TableFiles $tableFiles = new TableFiles(),
        public ?string $defaultBucket = null,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            tables: TablesList::fromArray($data['tables'] ?? []),
            files: FilesList::fromArray($data['files'] ?? []),
            tableFiles: TableFiles::fromArray($data['table_files'] ?? []),
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
        return $this->tables->isEmpty() && $this->files->isEmpty();
    }
}
