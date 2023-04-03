<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Exception;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\CommonExceptions\ExceptionWithContextInterface;
use RuntimeException;
use Throwable;

class ApplicationException extends RuntimeException implements
    ApplicationExceptionInterface,
    ExceptionWithContextInterface
{
    private readonly array $context;

    public function __construct(string $message, array $context = [], ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
        $this->context = $context;
    }


    public function getContext(): array
    {
        return $this->context;
    }
}
