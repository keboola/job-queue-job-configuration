<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\State\Storage\Files;

readonly class FileTag
{
    public function __construct(
        public string $name,
        public ?string $match = null,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            name: (string) $data['name'],
            match: $data['match'] ?? null,
        );
    }

    public function toArray(): array
    {
        $data = [
            'name' => $this->name,
        ];

        if ($this->match !== null) {
            $data['match'] = $this->match;
        }

        return $data;
    }
}
