<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Runtime;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Backend;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials\Type;
use PHPUnit\Framework\TestCase;

class BackendTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $backend = new Backend();

        self::assertNull($backend->type);
        self::assertNull($backend->context);
        self::assertNull($backend->workspaceCredentials);
    }

    public function testFromArray(): void
    {
        $data = [
            'type' => 'testType',
            'context' => 'testContext',
        ];

        $backend = Backend::fromArray($data);

        self::assertSame('testType', $backend->type);
        self::assertSame('testContext', $backend->context);
        self::assertNull($backend->workspaceCredentials);
    }

    public function testFromArrayWithWorkspaceCredentials(): void
    {
        $data = [
            'type' => 'testType',
            'context' => 'testContext',
            'workspace_credentials' => [
                'id' => 'workspace-123',
                'type' => 'snowflake',
                '#password' => 'secret-password',
            ],
        ];

        $backend = Backend::fromArray($data);

        self::assertSame('testType', $backend->type);
        self::assertSame('testContext', $backend->context);
        self::assertNotNull($backend->workspaceCredentials);
        self::assertSame('workspace-123', $backend->workspaceCredentials->id);
        self::assertSame(Type::SNOWFLAKE, $backend->workspaceCredentials->type);
        self::assertSame('secret-password', $backend->workspaceCredentials->password);
    }

    public function testToArray(): void
    {
        $backend = new Backend(
            type: 'testType',
            context: 'testContext',
        );

        self::assertSame([
            'type' => 'testType',
            'context' => 'testContext',
            'workspace_credentials' => null,
        ], $backend->toArray());
    }

    public function testToArrayWithWorkspaceCredentials(): void
    {
        $workspaceCredentials = WorkspaceCredentials::fromArray([
            'id' => 'workspace-123',
            'type' => 'snowflake',
            '#password' => 'secret-password',
        ]);

        $backend = new Backend(
            type: 'testType',
            context: 'testContext',
            workspaceCredentials: $workspaceCredentials,
        );

        self::assertSame([
            'type' => 'testType',
            'context' => 'testContext',
            'workspace_credentials' => [
                'id' => 'workspace-123',
                'type' => Type::SNOWFLAKE,
                '#password' => 'secret-password',
            ],
        ], $backend->toArray());
    }
}
