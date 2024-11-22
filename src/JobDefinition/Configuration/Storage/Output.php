<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\DataTypeSupport;

readonly class Output
{
    public function __construct(
        public TablesList $tables = new TablesList(),
        public FilesList $files = new FilesList(),
        public TableFiles $tableFiles = new TableFiles(),
        public ?string $defaultBucket = null,
        public ?DataTypeSupport $dataTypeSupport = null,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            tables: TablesList::fromArray($data['tables'] ?? []),
            files: FilesList::fromArray($data['files'] ?? []),
            tableFiles: TableFiles::fromArray($data['table_files'] ?? []),
            defaultBucket: isset($data['default_bucket']) ? (string) $data['default_bucket'] : null,
            dataTypeSupport: isset($data['data_type_support'])
                ? DataTypeSupport::from((string) $data['data_type_support'])
                : null,
        );
    }

    public function toArray(): array
    {
        $data = [
            'tables' => $this->tables->toArray(),
            'files' => $this->files->toArray(),
            'table_files' => $this->tableFiles->toArray(),
            'default_bucket' => $this->defaultBucket,
        ];

        if ($this->dataTypeSupport !== null) {
            $data['data_type_support'] = $this->dataTypeSupport;
        }

        return $data;
    }

    public function isEmpty(): bool
    {
        return $this->tables->isEmpty() && $this->files->isEmpty();
    }
}
