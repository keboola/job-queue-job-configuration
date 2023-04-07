<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage;

readonly class Storage
{
    public function __construct(
        public Input $input,
        public Output $output,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            input: Input::fromArray($data['input'] ?? []),
            output: Output::fromArray($data['output'] ?? []),
        );
    }

    public function toArray(): array
    {
        return [
            'input' => $this->input->toArray(),
            'output' => $this->output->toArray(),
        ];
    }
}
