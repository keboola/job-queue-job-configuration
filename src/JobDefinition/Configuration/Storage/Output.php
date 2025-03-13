<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\DataTypeSupport;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\TableModifications;

readonly class Output
{
    public function __construct(
        public TablesList $tables = new TablesList(),
        public FilesList $files = new FilesList(),
        public TableFiles $tableFiles = new TableFiles(),
        public ?string $defaultBucket = null,
        public ?DataTypeSupport $dataTypeSupport = null,
        public ?TableModifications $tableModifications = null,
        public ?array $treatValuesAsNull = null,
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
            tableModifications: isset($data['table_modifications'])
                ? TableModifications::from($data['table_modifications'])
                : null,
            treatValuesAsNull: $data['treat_values_as_null'] ?? null,
        );
    }

    public function toArray(): array
    {
        $data = [
            'tables' => $this->tables->toArray(),
            'files' => $this->files->toArray(),
            'table_files' => $this->tableFiles->toArray(),
            'default_bucket' => $this->defaultBucket,
            'table_modifications' => $this->tableModifications?->value,
            'treat_values_as_null' => $this->treatValuesAsNull,
        ];

        if ($this->dataTypeSupport !== null) {
            $data['data_type_support'] = $this->dataTypeSupport->value;
        }

        return $data;
    }

    public function isEmpty(): bool
    {
        return $this->tables->isEmpty() && $this->files->isEmpty();
    }
}
