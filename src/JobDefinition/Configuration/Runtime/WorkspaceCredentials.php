<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime;

use InvalidArgumentException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials\Type;

readonly class WorkspaceCredentials
{
    private function __construct(
        /** @var non-empty-string */
        public string $id,
        public Type $type,
        public ?string $password,
        public ?string $privateKey,
    ) {
        if (count(array_filter([$this->password, $this->privateKey])) !== 1) {
            throw new InvalidArgumentException(
                'Exactly one of "privateKey" and "password" must be configured in workspace_credentials',
            );
        }
    }

    /**
     * @param array{
     *   id: non-empty-string,
     *   type: value-of<Type>,
     *   "#password"?: string|null,
     *   "#privateKey"?: string|null,
     * } $credentials
     */
    public static function fromArray(array $credentials): self
    {
        return new self(
            id: (string) $credentials['id'],
            type: Type::from($credentials['type']),
            password: isset($credentials['#password']) ? ((string) $credentials['#password']) : null,
            privateKey: isset($credentials['#privateKey']) ? ((string) $credentials['#privateKey']) : null,
        );
    }

    public function toArray(): array
    {
        $data = [
            'id' => $this->id,
            'type' => $this->type,
        ];

        if ($this->password !== null) {
            $data['#password'] = $this->password;
        }

        if ($this->privateKey !== null) {
            $data['#privateKey'] = $this->privateKey;
        }

        return $data;
    }

    public function getCredentials(): array
    {
        $credentials = [];

        if ($this->password !== null) {
            $credentials['password'] = $this->password;
        }

        if ($this->privateKey !== null) {
            $credentials['privateKey'] = $this->privateKey;
        }

        return $credentials;
    }
}
