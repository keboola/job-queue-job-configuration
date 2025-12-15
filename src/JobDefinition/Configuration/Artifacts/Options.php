<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts;

readonly class Options
{
    public function __construct(
        public bool $zip = true,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            zip: (bool) ($data['zip'] ?? true),
        );
    }

    public function toArray(): array
    {
        return [
            'zip' => $this->zip,
        ];
    }
}
