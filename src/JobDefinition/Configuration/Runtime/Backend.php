<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime;

readonly class Backend
{
    public function __construct(
        public ?string $type = null,
        public ?string $context = null,
        public ?WorkspaceCredentials $workspaceCredentials = null,
    ) {
    }

    public static function fromArray(array $data): self
    {
        /** @var WorkspaceCredentialsArray|null $workspaceCredentials */
        $workspaceCredentials = $data['workspace_credentials'] ?? null;

        return new self(
            type: isset($data['type']) ? (string) $data['type'] : null,
            context: isset($data['context']) ? (string) $data['context'] : null,
            workspaceCredentials: $workspaceCredentials !== null ?
                WorkspaceCredentials::fromArray($workspaceCredentials) :
                null,
        );
    }

    public function toArray(): array
    {
        return [
            'type' => $this->type,
            'context' => $this->context,
            'workspace_credentials' => $this->workspaceCredentials?->toArray(),
        ];
    }
}
