<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Configuration;

use Keboola\JobQueue\JobConfiguration\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\Configuration\Runtime\Runtime;
use Keboola\JobQueue\JobConfiguration\Configuration\Storage\Storage;
use PHPUnit\Framework\TestCase;

class ConfigurationTest extends TestCase
{
    public function testFromArray(): void
    {
        $parameters = [
            'foo' => 'bar',
        ];

        $storage = [
            'input' => [
                'tables' => [
                    [
                        'source' => 'in.c-main.test',
                        'destination' => 'test.csv',
                    ],
                ],
            ],
        ];

        $processors = [
            'after' => [
                [
                    'definition' => [
                        'component' => 'foo',
                    ],
                ],
            ],
        ];

        $runtime = [
            'foo' => 'bar',
        ];

        $configuration = Configuration::fromArray([
            'parameters' => $parameters,
            'storage' => $storage,
            'processors' => $processors,
            'runtime' => $runtime,
        ]);

        self::assertSame($parameters, $configuration->parameters);
        self::assertEquals(Storage::fromArray($storage), $configuration->storage);
        self::assertSame($processors, $configuration->processors);
        self::assertEquals(Runtime::fromArray($runtime), $configuration->runtime);
    }
}
