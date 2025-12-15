<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage;

readonly class Input
{
    public function __construct(
        public TablesList $tables = new TablesList(),
        public FilesList $files = new FilesList(),
        public ?bool $readOnlyStorageAccess = null,
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
