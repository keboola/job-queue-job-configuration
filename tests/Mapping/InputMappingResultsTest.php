<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Input;
use Keboola\JobQueue\JobConfiguration\Mapping\InputMappingResults;
use PHPUnit\Framework\TestCase;

class InputMappingResultsTest extends TestCase
{
    private const array TABLES_STATE = [
        ['source' => 'in.c-bucket.table', 'lastImportDate' => '2026-08-25T12:00:00+0200'],
    ];
    private const array FILES_STATE = [
        ['tags' => [['name' => 'my-tag']], 'lastImportId' => '1234'],
    ];

    /**
     * What the input mapping step writes has to be what the service container reads back. They live in separate
     * applications, so nothing but this test connects the two ends of the format.
     */
    public function testEncodedStateDecodesToTheSameInput(): void
    {
        $encoded = InputMappingResults::encodeState(self::TABLES_STATE, self::FILES_STATE);

        // survives a trip through results.json, not just through memory
        $roundTripped = (array) json_decode((string) json_encode($encoded), true);

        self::assertEquals(
            Input::fromArray(['tables' => self::TABLES_STATE, 'files' => self::FILES_STATE]),
            InputMappingResults::decodeState($roundTripped),
        );
    }

    public function testEncodedStateHasTheShapeOfTheStoredNamespace(): void
    {
        self::assertSame(
            ['tables' => self::TABLES_STATE, 'files' => self::FILES_STATE],
            InputMappingResults::encodeState(self::TABLES_STATE, self::FILES_STATE),
        );
    }

    public function testEmptyListsStayEmptyLists(): void
    {
        self::assertSame(
            ['tables' => [], 'files' => []],
            InputMappingResults::encodeState([], []),
        );
    }
}
