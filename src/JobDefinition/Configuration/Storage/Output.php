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
        /** @var array $tables */
        $tables = $data['tables'] ?? [];
        /** @var array $files */
        $files = $data['files'] ?? [];
        /** @var array $tableFiles */
        $tableFiles = $data['table_files'] ?? [];
        /** @var array|null $treatValuesAsNull */
        $treatValuesAsNull = $data['treat_values_as_null'] ?? null;

        return new self(
            tables: TablesList::fromArray($tables),
            files: FilesList::fromArray($files),
            tableFiles: TableFiles::fromArray($tableFiles),
            defaultBucket: isset($data['default_bucket']) ? (string) $data['default_bucket'] : null,
            dataTypeSupport: isset($data['data_type_support'])
                ? DataTypeSupport::from((string) $data['data_type_support'])
                : null,
            tableModifications: isset($data['table_modifications'])
                ? TableModifications::from((string) $data['table_modifications'])
                : null,
            treatValuesAsNull: $treatValuesAsNull,
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
