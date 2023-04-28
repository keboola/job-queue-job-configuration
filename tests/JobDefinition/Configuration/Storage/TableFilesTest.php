<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\TableFiles;
use PHPUnit\Framework\TestCase;

class TableFilesTest extends TestCase
{
    public function testConstructor(): void
    {
        $tags = ['tag1', 'tag2'];
        $isPermanent = false;

        $tableFilesList = new TableFiles(
            tags: $tags,
            isPermanent: $isPermanent,
        );

        self::assertSame($tags, $tableFilesList->tags);
        self::assertSame($isPermanent, $tableFilesList->isPermanent);
    }

    public function testFromArray(): void
    {
        $tags = ['tag1', 'tag2'];
        $isPermanent = false;

        $tableFilesList = TableFiles::fromArray([
            'tags' => $tags,
            'is_permanent' => $isPermanent,
        ]);

        self::assertSame($tags, $tableFilesList->tags);
        self::assertSame($isPermanent, $tableFilesList->isPermanent);
    }

    public function testFromArrayDefaults(): void
    {
        $tableFilesList = TableFiles::fromArray([]);

        self::assertSame([], $tableFilesList->tags);
        self::assertTrue($tableFilesList->isPermanent);
    }

    public function testToArray(): void
    {
        $tableFilesList = new TableFiles(
            tags: ['tag1', 'tag2'],
            isPermanent: false,
        );

        self::assertSame([
            'tags' => ['tag1', 'tag2'],
            'is_permanent' => false,
        ], $tableFilesList->toArray());
    }
}
