<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\State\Storage\Files;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\File;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Files\FilesList;
use PHPUnit\Framework\TestCase;

class FilesListTest extends TestCase
{
    public function testFromToArray(): void
    {
        $file1 = File::fromArray([
            'tags' => [
                ['name' => ['foo', 'bar'], 'match' => 'extact'],
                ['name' => ['baz']],
            ],
            'lastImportId' => '123',
        ]);

        $file2 = File::fromArray([
            'tags' => [],
            'lastImportId' => '',
        ]);

        $filesList = new FilesList([$file1, $file2]);

        $this->assertSame([
            $file1->toArray(),
            $file2->toArray(),
        ], $filesList->toArray());
    }
}
