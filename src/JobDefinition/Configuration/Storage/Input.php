<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage;

readonly class Input
{
    public function __construct(
        public TablesList $tables,
        public FilesList $files,
        public ?bool $readOnlyStorageAccess,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            tables: TablesList::fromArray($data['tables'] ?? []),
            files: FilesList::fromArray($data['files'] ?? []),
            readOnlyStorageAccess: isset($data['read_only_storage_access']) ?
                (bool) $data['read_only_storage_access'] : null,
        );
    }

    public function toArray(): array
    {
        return [
            'tables' => $this->tables->toArray(),
            'files' => $this->files->toArray(),
            'read_only_storage_access' => $this->readOnlyStorageAccess,
        ];
    }

    public function isEmpty(): bool
    {
        return $this->tables->isEmpty() && $this->files->isEmpty();
    }
}
