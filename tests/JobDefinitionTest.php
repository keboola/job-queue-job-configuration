<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Component;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\JobDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
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
                    'type' => 'aws-ecr',
                    'uri' => '123456789.dkr.ecr.us-east-1.amazonaws.com/keboola.python-transformation-v2:latest',
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
