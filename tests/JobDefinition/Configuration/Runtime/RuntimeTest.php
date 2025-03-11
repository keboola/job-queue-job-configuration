<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Runtime;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Backend;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\Runtime;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials\Type;
use PHPUnit\Framework\TestCase;

class RuntimeTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $runtime = new Runtime();

        self::assertNull($runtime->safe);
        self::assertNull($runtime->imageTag);
        self::assertNull($runtime->processTimeout);
        self::assertNull($runtime->useFileStorageOnly);
        self::assertNull($runtime->backend);
        self::assertSame([], $runtime->extraProps);
    }

    public function testFromArray(): void
    {
        $data = [
            'safe' => true,
            'image_tag' => 'latest',
            'process_timeout' => 10,
            'use_file_storage_only' => true,
            'backend' => [
                'type' => 'testType',
                'context' => 'testContext',
            ],
            'foo' => 'bar',
        ];

        $runtime = Runtime::fromArray($data);

        self::assertTrue($runtime->safe);
        self::assertSame('latest', $runtime->imageTag);
        self::assertSame(10, $runtime->processTimeout);
        self::assertTrue($runtime->useFileStorageOnly);
        self::assertInstanceOf(Backend::class, $runtime->backend);
        self::assertSame('testType', $runtime->backend->type);
        self::assertSame('testContext', $runtime->backend->context);
        self::assertNull($runtime->backend->workspaceCredentials);
        self::assertSame(['foo' => 'bar'], $runtime->extraProps);
    }

    public function testFromArrayWithWorkspaceCredentials(): void
    {
        $data = [
            'safe' => true,
            'image_tag' => 'latest',
            'process_timeout' => 10,
            'use_file_storage_only' => true,
            'backend' => [
                'type' => 'testType',
                'context' => 'testContext',
                'workspace_credentials' => [
                    'id' => 'workspace-123',
                    'type' => 'snowflake',
                    '#password' => 'secret-password',
                ],
            ],
            'foo' => 'bar',
        ];

        $runtime = Runtime::fromArray($data);

        self::assertTrue($runtime->safe);
        self::assertSame('latest', $runtime->imageTag);
        self::assertSame(10, $runtime->processTimeout);
        self::assertTrue($runtime->useFileStorageOnly);
        self::assertInstanceOf(Backend::class, $runtime->backend);
        self::assertSame('testType', $runtime->backend->type);
        self::assertSame('testContext', $runtime->backend->context);
        self::assertNotNull($runtime->backend->workspaceCredentials);
        self::assertSame('workspace-123', $runtime->backend->workspaceCredentials->id);
        self::assertSame(Type::SNOWFLAKE, $runtime->backend->workspaceCredentials->type);
        self::assertSame('secret-password', $runtime->backend->workspaceCredentials->password);
        self::assertSame(['foo' => 'bar'], $runtime->extraProps);
    }

    public function testToArray(): void
    {
        $backend = new Backend(
            type: 'testType',
            context: 'testContext',
        );

        $runtime = new Runtime(
            safe: true,
            imageTag: 'latest',
            processTimeout: 20,
            useFileStorageOnly: true,
            backend: $backend,
            extraProps: ['foo' => 'bar'],
        );

        self::assertSame([
            'safe' => true,
            'image_tag' => 'latest',
            'process_timeout' => 20,
            'use_file_storage_only' => true,
            'backend' => [
                'type' => 'testType',
                'context' => 'testContext',
                'workspace_credentials' => null,
            ],
            'foo' => 'bar',
        ], $runtime->toArray());
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

        $runtime = new Runtime(
            safe: true,
            imageTag: 'latest',
            processTimeout: 20,
            useFileStorageOnly: true,
            backend: $backend,
            extraProps: ['foo' => 'bar'],
        );

        self::assertSame([
            'safe' => true,
            'image_tag' => 'latest',
            'process_timeout' => 20,
            'use_file_storage_only' => true,
            'backend' => [
                'type' => 'testType',
                'context' => 'testContext',
                'workspace_credentials' => [
                    'id' => 'workspace-123',
                    'type' => Type::SNOWFLAKE,
                    '#password' => 'secret-password',
                ],
            ],
            'foo' => 'bar',
        ], $runtime->toArray());
    }
}
