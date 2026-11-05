<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Logging;

use DateTimeImmutable;
use Keboola\JobQueue\JobConfiguration\Logging\GelfFormatter;
use Keboola\StorageApi\ClientException;
use LogicException;
use Monolog\Level;
use Monolog\LogRecord;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class GelfFormatterTest extends TestCase
{
    public function testFormatsRecordWithoutException(): void
    {
        $message = (new GelfFormatter('pod-1'))->format($this->createRecord(
            Level::Info,
            'Fetched table in.c-main.a',
            ['table' => 'in.c-main.a', 'rows' => 10],
        ));

        self::assertSame('Fetched table in.c-main.a', $message->getShortMessage());
        self::assertSame(6, $message->getSyslogLevel());
        self::assertSame('pod-1', $message->getHost());
        self::assertNull($message->getFullMessage());
        self::assertSame(
            ['facility' => 'input-mapping', 'table' => 'in.c-main.a', 'rows' => 10],
            $message->getAllAdditionals(),
        );
    }

    public function testFormatsExceptionAsErrorDetails(): void
    {
        $storageApiException = new ClientException(
            'Table not found',
            404,
            null,
            'storage.tables.notFound',
            ['exceptionId' => 'connection-abc123', 'errorCode' => 'storage.tables.notFound'],
        );
        $exception = new RuntimeException('Failed to load input', 0, $storageApiException);

        $message = (new GelfFormatter('pod-1'))->format($this->createRecord(
            Level::Error,
            'Failed to load input',
            ['exception' => $exception, 'errorType' => 'user'],
        ));

        self::assertSame('Failed to load input', $message->getShortMessage());
        self::assertSame(3, $message->getSyslogLevel());

        $additionals = $message->getAllAdditionals();
        self::assertSame('input-mapping', $additionals['facility']);
        self::assertSame('user', $additionals['errorType']);
        self::assertSame(RuntimeException::class, $additionals['error.kind']);
        self::assertSame('Failed to load input', $additionals['error.message']);
        self::assertSame('connection-abc123', $additionals['exceptionId']);
        self::assertSame(__FILE__, $additionals['file']);
        self::assertSame($exception->getLine(), $additionals['line']);
        self::assertArrayNotHasKey('exception', $additionals);

        $trace = $message->getFullMessage();
        self::assertNotNull($trace);
        self::assertStringStartsWith(
            sprintf('%s: Failed to load input in %s:%d', RuntimeException::class, __FILE__, $exception->getLine()),
            $trace,
        );
        self::assertStringContainsString(
            sprintf(
                "\n\nCaused by %s: Table not found in %s:%d",
                ClientException::class,
                __FILE__,
                $storageApiException->getLine(),
            ),
            $trace,
        );
        self::assertStringContainsString('#0 ', $trace);
    }

    public function testExceptionWithoutStorageApiErrorHasNoExceptionId(): void
    {
        $message = (new GelfFormatter('pod-1'))->format($this->createRecord(
            Level::Critical,
            'Internal error',
            ['exception' => new LogicException('boom'), 'errorType' => 'application'],
        ));

        $additionals = $message->getAllAdditionals();
        self::assertArrayNotHasKey('exceptionId', $additionals);
        self::assertSame(LogicException::class, $additionals['error.kind']);
        self::assertSame('boom', $additionals['error.message']);
        self::assertSame(2, $message->getSyslogLevel());
    }

    public function testTraceIsCutToMaxLength(): void
    {
        $trace = GelfFormatter::trace(new RuntimeException(str_repeat('x', 2 * GelfFormatter::MAX_TRACE_LENGTH)));

        self::assertSame(GelfFormatter::MAX_TRACE_LENGTH + 4, mb_strlen($trace));
        self::assertStringEndsWith(' ...', $trace);
    }

    public function testStorageApiExceptionIdIgnoresEmptyValues(): void
    {
        self::assertNull(
            GelfFormatter::storageApiExceptionId(new ClientException('x', 500, null, null, ['exceptionId' => ''])),
        );
        self::assertNull(GelfFormatter::storageApiExceptionId(new ClientException('x', 500, null, null, null)));
        self::assertSame(
            '42',
            GelfFormatter::storageApiExceptionId(new ClientException('x', 500, null, null, ['exceptionId' => 42])),
        );
    }

    private function createRecord(Level $level, string $message, array $context): LogRecord
    {
        return new LogRecord(
            new DateTimeImmutable('2026-11-05T10:00:00Z'),
            'input-mapping',
            $level,
            $message,
            $context,
        );
    }
}
