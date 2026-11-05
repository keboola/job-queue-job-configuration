<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Logging;

use Gelf\Message;
use Keboola\StorageApi\Exception as StorageApiException;
use Monolog\Formatter\GelfMessageFormatter;
use Monolog\LogRecord;
use Throwable;

/**
 * Formats a record for the job-queue GELF server. The exception in the context becomes the error details the
 * server forwards to Datadog: full_message carries the trace, _error.kind and _error.message the exception,
 * _exceptionId the Storage API exception id when one is in the chain. The remaining context keys travel as
 * additional fields without a prefix, so a context key named "errorType" arrives as _errorType.
 */
final class GelfFormatter extends GelfMessageFormatter
{
    public const MAX_TRACE_LENGTH = 16 * 1024;

    private const CONTEXT_EXCEPTION = 'exception';

    public function __construct(?string $systemName = null)
    {
        parent::__construct(systemName: $systemName, contextPrefix: '');
    }

    public function format(LogRecord $record): Message
    {
        $exception = $record->context[self::CONTEXT_EXCEPTION] ?? null;
        if (!$exception instanceof Throwable) {
            return parent::format($record);
        }

        $context = $record->context;
        unset($context[self::CONTEXT_EXCEPTION]);
        $message = parent::format($record->with(context: $context));

        $message->setFullMessage(self::trace($exception));
        $message->setAdditional('error.kind', $exception::class);
        $message->setAdditional('error.message', $exception->getMessage());
        $message->setAdditional('file', $exception->getFile());
        $message->setAdditional('line', $exception->getLine());

        $exceptionId = self::storageApiExceptionId($exception);
        if ($exceptionId !== null) {
            $message->setAdditional('exceptionId', $exceptionId);
        }

        return $message;
    }

    /**
     * The exception and the ones it wraps, each with its trace, cut to MAX_TRACE_LENGTH.
     */
    public static function trace(Throwable $exception): string
    {
        $parts = [];
        for ($e = $exception; $e !== null; $e = $e->getPrevious()) {
            $parts[] = sprintf(
                "%s: %s in %s:%d\n%s",
                $e::class,
                $e->getMessage(),
                $e->getFile(),
                $e->getLine(),
                $e->getTraceAsString(),
            );
        }

        $trace = implode("\n\nCaused by ", $parts);
        if (mb_strlen($trace) > self::MAX_TRACE_LENGTH) {
            $trace = mb_substr($trace, 0, self::MAX_TRACE_LENGTH) . ' ...';
        }

        return $trace;
    }

    /**
     * The Storage API returns an exception id with every error; the client keeps it in the exception's context.
     */
    public static function storageApiExceptionId(Throwable $exception): ?string
    {
        for ($e = $exception; $e !== null; $e = $e->getPrevious()) {
            if (!$e instanceof StorageApiException) {
                continue;
            }
            $params = $e->getContextParams();
            if (is_array($params) && is_scalar($params['exceptionId'] ?? null) && $params['exceptionId'] !== '') {
                return (string) $params['exceptionId'];
            }
        }

        return null;
    }
}
