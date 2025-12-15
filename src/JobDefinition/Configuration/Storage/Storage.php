<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage;

readonly class Storage
{
    public function __construct(
        public Input $input = new Input(),
        public Output $output = new Output(),
    ) {
    }

    public static function fromArray(array $data): self
    {
        /** @var array $input */
        $input = $data['input'] ?? [];
        /** @var array $output */
        $output = $data['output'] ?? [];

        return new self(
            input: Input::fromArray($input),
            output: Output::fromArray($output),
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
