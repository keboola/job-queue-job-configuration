<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors;

readonly class ProcessorDefinition
{
    /**
     * @param non-empty-string $component
     * @param ?non-empty-string $tag
     */
    public function __construct(
        public string $component,
        public ?string $tag = null,
    ) {
    }

    /**
     * @param array{
     *     component: non-empty-string,
     *     tag?: ?string,
     * } $data
     */
    public static function fromArray(array $data): self
    {
        $tag = isset($data['tag']) ? (string) $data['tag'] : null;
        if ($tag === '') {
            $tag = null;
        }

        return new self(
            component: (string) $data['component'],
            tag: $tag,
        );
    }

    /**
     * @return array{
     *     component: non-empty-string,
     *     tag?: non-empty-string,
     * }
     */
    public function toArray(): array
    {
        $data = [
            'component' => $this->component,
        ];

        if ($this->tag !== null) {
            $data['tag'] = $this->tag;
        }

        return $data;
    }
}
