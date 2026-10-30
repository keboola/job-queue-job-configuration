<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tracing;

use JsonException;
use RuntimeException;
use function DDTrace\consume_distributed_tracing_headers;
use function DDTrace\generate_distributed_tracing_headers;

/**
 * Carries the Datadog trace context from the service container to a step container through the step data dir,
 * so the step's own trace continues under the service container span that runs the step.
 *
 * The step process has to import the context before it opens any span of its own: the context is applied to its root
 * span only. The service container exports it before it unblocks the step, and the step process is not started before
 * that (command-wrapper waits for the previous step status).
 */
final class StepTraceContext
{
    public const FILE_NAME = 'trace-context.json';

    public static function export(string $dataDirPath): void
    {
        if (!function_exists('DDTrace\generate_distributed_tracing_headers')) {
            return;
        }

        self::write($dataDirPath, generate_distributed_tracing_headers());
    }

    public static function import(string $dataDirPath): void
    {
        if (!function_exists('DDTrace\consume_distributed_tracing_headers')) {
            return;
        }

        $headers = self::read($dataDirPath);
        if ($headers !== []) {
            consume_distributed_tracing_headers($headers);
        }
    }

    /**
     * @param array<string, string> $headers
     */
    public static function write(string $dataDirPath, array $headers): void
    {
        if ($headers === []) {
            return;
        }

        $filePath = self::getFilePath($dataDirPath);
        if (file_put_contents($filePath, json_encode($headers, JSON_THROW_ON_ERROR)) === false) {
            throw new RuntimeException(sprintf('Failed to write trace context file "%s"', $filePath));
        }
    }

    /**
     * A missing or malformed file yields no headers, tracing must never fail the step.
     *
     * @return array<string, string>
     */
    public static function read(string $dataDirPath): array
    {
        $filePath = self::getFilePath($dataDirPath);
        if (!is_file($filePath)) {
            return [];
        }

        $content = @file_get_contents($filePath);
        if ($content === false) {
            return [];
        }

        try {
            $data = json_decode($content, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return [];
        }

        if (!is_array($data)) {
            return [];
        }

        return array_filter(
            $data,
            fn(mixed $value, mixed $key) => is_string($key) && is_string($value),
            ARRAY_FILTER_USE_BOTH,
        );
    }

    private static function getFilePath(string $dataDirPath): string
    {
        return rtrim($dataDirPath, '/') . '/' . self::FILE_NAME;
    }
}
