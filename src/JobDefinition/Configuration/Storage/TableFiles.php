<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage;

readonly class TableFiles
{
    public function __construct(
        /** @var $tags string[] */
        public array $tags = [],
        public bool $isPermanent = true,
    ) {
    }

    public static function fromArray(array $data): self
    {
        /** @var array $tags */
        $tags = $data['tags'] ?? [];

        return new self(
            tags: $tags,
            isPermanent: (bool) ($data['is_permanent'] ?? true),
        );
    }

    public function toArray(): array
    {
        return [
            'tags' => $this->tags,
            'is_permanent' => $this->isPermanent,
        ];
    }
}
