<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Logging;

use Keboola\JobQueue\JobConfiguration\Logging\GelfHandlerFactory;
use Monolog\Level;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class GelfHandlerFactoryTest extends TestCase
{
    /** @var resource */
    private $server;

    private int $port;

    protected function setUp(): void
    {
        parent::setUp();

        $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        self::assertNotFalse($server, $errstr);
        $this->server = $server;

        $name = stream_socket_get_name($server, false);
        self::assertNotFalse($name);
        $this->port = (int) substr($name, (int) strrpos($name, ':') + 1);
    }

    protected function tearDown(): void
    {
        fclose($this->server);
        putenv(GelfHandlerFactory::ENV_ADDRESS);
        putenv(GelfHandlerFactory::ENV_PORT);

        parent::tearDown();
    }

    public function testCreateFromEnvironmentWithoutAddress(): void
    {
        self::assertNull(GelfHandlerFactory::createFromEnvironment(Level::Info));

        putenv(GelfHandlerFactory::ENV_ADDRESS . '=127.0.0.1');
        self::assertNull(GelfHandlerFactory::createFromEnvironment(Level::Info), 'port missing');

        putenv(GelfHandlerFactory::ENV_PORT . '=not-a-port');
        self::assertNull(GelfHandlerFactory::createFromEnvironment(Level::Info), 'port not numeric');
    }

    public function testHandlerSendsFormattedMessageOverTcp(): void
    {
        putenv(GelfHandlerFactory::ENV_ADDRESS . '=127.0.0.1');
        putenv(GelfHandlerFactory::ENV_PORT . '=' . $this->port);

        $handler = GelfHandlerFactory::createFromEnvironment(Level::Info);
        self::assertNotNull($handler);

        $logger = new Logger('input-mapping', [$handler]);
        $logger->debug('Below the handler level');
        $logger->error('Table not found', ['exception' => new RuntimeException('boom'), 'errorType' => 'user']);

        $messages = $this->readMessages();
        self::assertCount(1, $messages);
        $message = $messages[0];
        self::assertSame('1.1', $message['version']);
        self::assertSame('Table not found', $message['short_message']);
        self::assertSame(3, $message['level']);
        self::assertSame('user', $message['_errorType']);
        self::assertSame(RuntimeException::class, $message['_error.kind']);
        self::assertSame('boom', $message['_error.message']);
        self::assertIsString($message['full_message']);
        self::assertStringStartsWith(RuntimeException::class . ': boom in ', $message['full_message']);
        self::assertSame(__FILE__, $message['_file']);
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function readMessages(): array
    {
        $conn = stream_socket_accept($this->server, 2);
        self::assertNotFalse($conn, 'the handler did not connect');
        stream_set_timeout($conn, 2);

        $data = '';
        while (!str_ends_with($data, "\0")) {
            $chunk = fread($conn, 65536);
            self::assertNotFalse($chunk);
            if ($chunk === '') {
                break;
            }
            $data .= $chunk;
        }
        fclose($conn);

        $messages = [];
        foreach (explode("\0", rtrim($data, "\0")) as $raw) {
            /** @var array<string, mixed> $decoded */
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            $messages[] = $decoded;
        }

        return $messages;
    }
}
