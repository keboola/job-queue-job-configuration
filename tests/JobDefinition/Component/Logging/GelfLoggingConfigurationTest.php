<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Component\Logging;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Logging\GelfLoggingConfiguration;
use PHPUnit\Framework\TestCase;

class GelfLoggingConfigurationTest extends TestCase
{
    public function testConstruct(): void
    {
        $config = new GelfLoggingConfiguration('tcp');
        self::assertSame('tcp', $config->type);
        self::assertSame('12001', $config->port);

        $config = new GelfLoggingConfiguration('udp');
        self::assertSame('udp', $config->type);
        self::assertSame('12001', $config->port);

        $config = new GelfLoggingConfiguration('http');
        self::assertSame('http', $config->type);
        self::assertSame('12002', $config->port);
    }

    public function testFromArray(): void
    {
        $data = [
            'gelf_server_type' => 'tcp',
        ];

        $config = GelfLoggingConfiguration::fromArray($data);

        self::assertSame('tcp', $config->type);
    }

    public function testDefaultsFromArray(): void
    {
        $data = [];

        $config = GelfLoggingConfiguration::fromArray($data);

        self::assertSame('tcp', $config->type);
    }

    public function testToArray(): void
    {
        $config = new GelfLoggingConfiguration('tcp');

        self::assertSame([
            'type' => 'gelf',
            'gelf_server_type' => 'tcp',
        ], $config->toArray());
    }
}
