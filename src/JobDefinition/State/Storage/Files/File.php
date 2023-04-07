<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files;

readonly class File
{
    public function __construct(
        /** @var $tags FileTag[] */
        public array $tags,
        public string $lastImportId,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            tags: array_map(FileTag::fromArray(...), $data['tags'] ?? []),
            lastImportId: (string) $data['lastImportId'],
        );
    }

    public function toArray(): array
    {
        return [
            'tags' => array_map(fn (FileTag $tag) => $tag->toArray(), $this->tags),
            'lastImportId' => $this->lastImportId,
        ];
    }
}
