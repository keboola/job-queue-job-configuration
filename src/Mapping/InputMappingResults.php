<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Mapping;

use Keboola\JobQueue\JobConfiguration\JobDefinition\State\Storage\Input;

/**
 * The `results.json` contract between the input mapping step and the service container that reads it.
 *
 * The two live in separate applications with no dependency on each other, so this shared library is the only
 * place that can hold both the key and the shape. Both sides go through here, which is what keeps a rename on
 * one side from silently going unnoticed on the other.
 */
final class InputMappingResults
{
    /** Key the input mapping step reports its state under. */
    public const string STATE_KEY = 'inputState';

    /**
     * @param array $tablesState Serialized InputTableStateList.
     * @param array $filesState Serialized InputFileStateList.
     */
    public static function encodeState(array $tablesState, array $filesState): array
    {
        // routed through Input so that the emitted shape is the one the reader parses, not a parallel literal
        return Input::fromArray([
            'tables' => $tablesState,
            'files' => $filesState,
        ])->toArray();
    }

    public static function decodeState(array $state): Input
    {
        return Input::fromArray($state);
    }
}
