<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime;

use InvalidArgumentException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials\Type;
use ValueError;

readonly class WorkspaceCredentials
{
    private function __construct(
        public string $id,
        public Type $type,
        public string $password,
    ) {
    }

    public static function fromArray(array $credentials): self
    {
        if (!isset($credentials['id']) || !isset($credentials['type']) || !isset($credentials['#password'])) {
            throw new InvalidArgumentException(
                'Missing required fields (id, type, #password) in workspace_credentials',
            );
        }

        try {
            $type = Type::from($credentials['type']);
        } catch (ValueError $e) {
            throw new InvalidArgumentException(
                sprintf('Unsupported workspace type "%s"', $credentials['type']),
                previous: $e,
            );
        }

        return new self(
            id: (string) $credentials['id'],
            type: $type,
            password: (string) $credentials['#password'],
        );
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'type' => $this->type,
            '#password' => $this->password,
        ];
    }
}
