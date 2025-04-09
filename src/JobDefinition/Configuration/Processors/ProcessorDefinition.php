<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors;

readonly class ProcessorDefinition
{
    /**
     * @param non-empty-string $component
     */
    public function __construct(
        public string $component,
    ) {
    }

    /**
     * @param array{
     *     component: non-empty-string
     * } $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            component: (string) $data['component'],
        );
    }

    public function toArray(): array
    {
        return [
            'component' => $this->component,
        ];
    }
}
