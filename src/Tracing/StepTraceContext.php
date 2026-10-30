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
final readonly class StepTraceContext
{
    public const FILE_NAME = 'trace-context.json';

    public function __construct(
        private string $dataDirPath,
    ) {
    }

    public function export(): void
    {
        if (!function_exists('DDTrace\generate_distributed_tracing_headers')) {
            return;
        }

        $this->write(generate_distributed_tracing_headers());
    }

    /**
     * A missing or malformed file is ignored, tracing must never fail the step.
     */
    public function import(): void
    {
        if (!function_exists('DDTrace\consume_distributed_tracing_headers')) {
            return;
        }

        $headers = $this->read();
        if ($headers !== []) {
            consume_distributed_tracing_headers($headers);
        }
    }

    /**
     * @param array<string, string> $headers
     */
    private function write(array $headers): void
    {
        if ($headers === []) {
            return;
        }

        $filePath = $this->getFilePath();
        if (file_put_contents($filePath, json_encode($headers, JSON_THROW_ON_ERROR)) === false) {
            throw new RuntimeException(sprintf('Failed to write trace context file "%s"', $filePath));
        }
    }

    /**
     * @return array<string, string>
     */
    private function read(): array
    {
        $filePath = $this->getFilePath();
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

    private function getFilePath(): string
    {
        return rtrim($this->dataDirPath, '/') . '/' . self::FILE_NAME;
    }
}
