<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime;

readonly class Runtime
{
    public function __construct(
        public ?bool $safe = null,
        public ?bool $imageTag = null,
        public ?bool $useFileStorageOnly = null,
        public ?Backend $backend = null,
        public array $extraProps = [],
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            safe: $data['safe'] ?? null,
            imageTag: $data['image_tag'] ?? null,
            useFileStorageOnly: isset($data['use_file_storage_only']) ? (bool) $data['use_file_storage_only'] : null,
            backend: isset($data['backend']) ? Backend::fromArray($data['backend']) : null,
            extraProps: array_diff_key($data, array_flip([
                'safe',
                'image_tag',
                'use_file_storage_only',
                'backend',
            ])),
        );
    }

    public function toArray(): array
    {
        return array_merge([
            'safe' => $this->safe,
            'image_tag' => $this->imageTag,
            'use_file_storage_only' => $this->useFileStorageOnly,
            'backend' => $this->backend?->toArray(),
        ], $this->extraProps);
    }
}
