<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime;

readonly class Runtime
{
    public function __construct(
        public ?bool $safe = null,
        public ?string $imageTag = null,
        /** @var positive-int|null */
        public ?int $processTimeout = null,
        public ?bool $useFileStorageOnly = null,
        public ?Backend $backend = null,
        public array $extraProps = [],
    ) {
    }

    public static function fromArray(array $data): self
    {
        /** @var array|null $backend */
        $backend = $data['backend'] ?? null;

        return new self(
            safe: isset($data['safe']) ? (bool) $data['safe'] : null,
            imageTag: isset($data['image_tag']) ? (string) $data['image_tag'] : null,
            processTimeout: isset($data['process_timeout']) ? (int) $data['process_timeout'] : null,
            useFileStorageOnly: isset($data['use_file_storage_only']) ? (bool) $data['use_file_storage_only'] : null,
            backend: $backend !== null ? Backend::fromArray($backend) : null,
            extraProps: array_diff_key($data, array_flip([
                'safe',
                'image_tag',
                'process_timeout',
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
            'process_timeout' => $this->processTimeout,
            'use_file_storage_only' => $this->useFileStorageOnly,
            'backend' => $this->backend?->toArray(),
        ], $this->extraProps);
    }
}
