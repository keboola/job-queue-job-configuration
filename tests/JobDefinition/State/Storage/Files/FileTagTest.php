<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State\Storage\Files;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FileTag;
use PHPUnit\Framework\TestCase;

class FileTagTest extends TestCase
{
    public function testConstructor(): void
    {
        $name = 'tag1';
        $match = 'file*.csv';
        $tag = new FileTag($name, $match);

        self::assertSame($name, $tag->name);
        self::assertSame($match, $tag->match);
    }

    public function testFromArray(): void
    {
        $name = 'tag1';
        $match = 'file*.csv';
        $data = [
            'name' => $name,
            'match' => $match,
        ];

        $tag = FileTag::fromArray($data);

        self::assertSame($name, $tag->name);
        self::assertSame($match, $tag->match);
    }

    public function testFromArrayWithNullMatch(): void
    {
        $name = 'tag1';
        $data = [
            'name' => $name,
            'match' => null,
        ];

        $tag = FileTag::fromArray($data);

        self::assertSame($name, $tag->name);
        self::assertNull($tag->match);
    }

    public function testToArray(): void
    {
        $name = 'tag1';
        $match = 'file*.csv';
        $tag = new FileTag($name, $match);

        $expectedData = [
            'name' => $name,
            'match' => $match,
        ];
        $data = $tag->toArray();

        self::assertSame($expectedData, $data);
    }

    public function testToArrayWithNullMatch(): void
    {
        $name = 'tag1';
        $tag = new FileTag($name, null);

        $expectedData = [
            'name' => $name,
        ];
        $data = $tag->toArray();

        self::assertSame($expectedData, $data);
    }
}
