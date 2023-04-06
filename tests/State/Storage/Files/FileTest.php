<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\State\Storage\Files;

use Keboola\JobQueue\JobConfiguration\State\Storage\Files\File;
use Keboola\JobQueue\JobConfiguration\State\Storage\Files\FileTag;
use PHPUnit\Framework\TestCase;

class FileTest extends TestCase
{
    public function testConstructor(): void
    {
        $tags = [
            new FileTag('tag1', 'file*.csv'),
            new FileTag('tag2', null),
        ];
        $lastImportId = '123';

        $file = new File($tags, $lastImportId);

        self::assertSame($tags, $file->tags);
        self::assertSame($lastImportId, $file->lastImportId);
    }

    public function testFromArray(): void
    {
        $tagsData = [
            [
                'name' => 'tag1',
                'match' => 'file*.csv',
            ],
            [
                'name' => 'tag2',
            ],
        ];
        $data = [
            'tags' => $tagsData,
            'lastImportId' => '123',
        ];

        $file = File::fromArray($data);

        $tags = [
            new FileTag('tag1', 'file*.csv'),
            new FileTag('tag2', null),
        ];
        self::assertEquals($tags, $file->tags);
        self::assertSame($data['lastImportId'], $file->lastImportId);
    }

    public function testFromArrayWithEmptyTags(): void
    {
        $data = [
            'tags' => [],
            'lastImportId' => '123',
        ];

        $file = File::fromArray($data);

        self::assertEmpty($file->tags);
        self::assertSame($data['lastImportId'], $file->lastImportId);
    }

    public function testToArray(): void
    {
        $tags = [
            new FileTag('tag1', 'file*.csv'),
            new FileTag('tag2', null),
        ];
        $lastImportId = '123';
        $file = new File($tags, $lastImportId);

        $expectedData = [
            'tags' => [
                [
                    'name' => 'tag1',
                    'match' => 'file*.csv',
                ],
                [
                    'name' => 'tag2',
                ],
            ],
            'lastImportId' => $lastImportId,
        ];
        $data = $file->toArray();

        self::assertSame($expectedData, $data);
    }

    public function testToArrayWithEmptyTags(): void
    {
        $tags = [];
        $lastImportId = '123';
        $file = new File($tags, $lastImportId);

        $expectedData = [
            'tags' => [],
            'lastImportId' => $lastImportId,
        ];
        $data = $file->toArray();

        self::assertSame($expectedData, $data);
    }
}
