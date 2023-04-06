<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests;

use Keboola\JobQueue\JobConfiguration\Component;
use Keboola\JobQueue\JobConfiguration\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition;
use Keboola\JobQueue\JobConfiguration\State\State;
use PHPUnit\Framework\TestCase;

class JobDefinitionTest extends TestCase
{
    public function testConstructor(): void
    {
        $component = new Component([
            'id' => 'keboola.python-transformation-v2',
            'data' => [
                'network' => 'bridge',
                'definition' => [
                    'type' => 'transformation',
                ],
            ],
        ]);

        $configuration = Configuration::fromArray([]);
        $state = State::fromArray([]);

        $jobDefinition = new JobDefinition(
            $component,
            'config1',
            'row1',
            $configuration,
            $state,
        );

        self::assertSame($component, $jobDefinition->component);
        self::assertSame('config1', $jobDefinition->configId);
        self::assertSame('row1', $jobDefinition->rowId);
        self::assertSame($configuration, $jobDefinition->configuration);
        self::assertSame($state, $jobDefinition->state);
    }
}
