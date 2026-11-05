<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Logging;

use Gelf\Publisher;
use Gelf\Transport\TcpTransport;
use Monolog\Handler\GelfHandler;
use Monolog\Level;

/**
 * Builds the handler that sends the records to the job-queue GELF server at KBC_LOGGER_ADDR:KBC_LOGGER_PORT, the
 * address the job queue injects into a step whose component logs through GELF (the common interface logging
 * specification). Without the address there is no server to talk to and no handler is built.
 */
final class GelfHandlerFactory
{
    public const ENV_ADDRESS = 'KBC_LOGGER_ADDR';
    public const ENV_PORT = 'KBC_LOGGER_PORT';

    public static function createFromEnvironment(Level $level): ?GelfHandler
    {
        $address = getenv(self::ENV_ADDRESS);
        $port = getenv(self::ENV_PORT);
        if (!is_string($address) || $address === '' || !is_string($port) || !ctype_digit($port)) {
            return null;
        }

        return self::create($address, (int) $port, $level);
    }

    public static function create(string $address, int $port, Level $level): GelfHandler
    {
        $publisher = new Publisher(new TcpTransport($address, $port));

        $handler = new GelfHandler($publisher, $level);
        $handler->setFormatter(new GelfFormatter());

        return $handler;
    }
}
