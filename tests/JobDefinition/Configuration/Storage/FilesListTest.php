<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Storage;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Storage\FilesList;
use PHPUnit\Framework\TestCase;

class FilesListTest extends TestCase
{
    public function testFromArray(): void
    {
        $data = [
            [
                'key' => 'foo/bar.csv',
                'tags' => [],
            ],
            [
                'key' => 'foo/baz.csv',
                'tags' => [],
            ],
        ];

        $filesList = FilesList::fromArray($data);

        self::assertCount(2, $filesList);
        self::assertSame($data, $filesList->toArray());
    }

    public function testCount(): void
    {
        $filesList = new FilesList([
            [
                'key' => 'foo/bar.csv',
                'tags' => [],
            ],
            [
                'key' => 'foo/baz.csv',
                'tags' => [],
            ],
        ]);

        self::assertSame(2, $filesList->count());
    }

    public function testIsEmpty(): void
    {
        $filesList = new FilesList([]);

        self::assertTrue($filesList->isEmpty());

        $filesList = new FilesList([
            [
                'key' => 'foo/bar.csv',
                'tags' => [],
            ],
        ]);

        self::assertFalse($filesList->isEmpty());
    }

    public function testToArray(): void
    {
        $data = [
            [
                'key' => 'foo/bar.csv',
                'tags' => [],
            ],
            [
                'key' => 'foo/baz.csv',
                'tags' => [],
            ],
        ];

        $filesList = new FilesList($data);

        self::assertSame($data, $filesList->toArray());
    }
}
