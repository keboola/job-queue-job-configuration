<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage;

readonly class Storage
{
    public function __construct(
        public Input $input,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            input: Input::fromArray($data['input'] ?? []),
        );
    }

    public function toArray(): array
    {
        return [
            'input' => $this->input->toArray(),
        ];
    }
}
