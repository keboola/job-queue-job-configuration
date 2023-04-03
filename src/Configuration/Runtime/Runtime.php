<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Configuration\Runtime;

readonly class Runtime
{
    public function __construct(
        public ?bool $safe,
        public ?bool $imageTag,
        public ?bool $useFileStorageOnly,
        public ?Backend $backend,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            safe: $data['safe'] ?? null,
            imageTag: $data['image_tag'] ?? null,
            useFileStorageOnly: isset($data['use_file_storage_only']) ? (bool) $data['use_file_storage_only'] : null,
            backend: isset($data['backend']) ? Backend::fromArray($data['backend']) : null,
        );
    }

    public function toArray(): array
    {
        $data = [];

        if ($this->safe !== null) {
            $data['safe'] = $this->safe;
        }

        if ($this->imageTag !== null) {
            $data['image_tag'] = $this->imageTag;
        }

        if ($this->useFileStorageOnly !== null) {
            $data['use_file_storage_only'] = $this->useFileStorageOnly;
        }

        if ($this->backend !== null) {
            $data['backend'] = $this->backend->toArray();
        }

        return $data;
    }
}
