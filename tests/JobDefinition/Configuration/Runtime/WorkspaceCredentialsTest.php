<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Runtime;

use InvalidArgumentException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials\Type;
use PHPUnit\Framework\TestCase;
use ValueError;

class WorkspaceCredentialsTest extends TestCase
{
    public function testFromArray(): void
    {
        $data = [
            'id' => 'workspace-123',
            'type' => 'snowflake',
            '#password' => 'secret-password',
        ];

        $credentials = WorkspaceCredentials::fromArray($data);

        self::assertSame('workspace-123', $credentials->id);
        self::assertSame(Type::SNOWFLAKE, $credentials->type);
        self::assertSame('secret-password', $credentials->password);
    }

    public function testToArray(): void
    {
        $data = [
            'id' => 'workspace-123',
            'type' => 'snowflake',
            '#password' => 'secret-password',
        ];

        $credentials = WorkspaceCredentials::fromArray($data);
        $result = $credentials->toArray();

        self::assertSame([
            'id' => 'workspace-123',
            'type' => Type::SNOWFLAKE,
            '#password' => 'secret-password',
        ], $result);
    }
}
