<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage;

readonly class Storage
{
    public function __construct(
        public Input $input = new Input(),
    ) {
    }

    public static function fromArray(array $data): self
    {
        /** @var array $input */
        $input = $data['input'] ?? [];

        return new self(
            input: Input::fromArray($input),
        );
    }

    public function toArray(): array
    {
        return [
            'input' => $this->input->toArray(),
        ];
    }
}
