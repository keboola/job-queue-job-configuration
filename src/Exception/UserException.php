<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Exception;

use Keboola\CommonExceptions\ExceptionWithContextInterface;
use Keboola\CommonExceptions\UserExceptionInterface;
use RuntimeException;
use Throwable;

class UserException extends RuntimeException implements
    UserExceptionInterface,
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
