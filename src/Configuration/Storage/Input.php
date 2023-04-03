<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Configuration\Storage;

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
        $data = [
            'tables' => $this->tables->toArray(),
            'files' => $this->files->toArray(),
        ];

        if ($this->readOnlyStorageAccess !== null) {
            $data['read_only_storage_access'] = $this->readOnlyStorageAccess;
        }

        return $data;
    }

    public function isEmpty(): bool
    {
        return $this->tables->isEmpty() && $this->files->isEmpty();
    }
}
