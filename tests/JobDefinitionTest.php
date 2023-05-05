<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\JobDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use PHPUnit\Framework\TestCase;

class JobDefinitionTest extends TestCase
{
    public function testConstructor(): void
    {
        $component = new ComponentSpecification([
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
            component: $component,
            configId: 'config1',
            rowId: 'row1',
            isDisabled: false,
            configuration: $configuration,
            state: $state,
        );

        self::assertSame($component, $jobDefinition->component);
        self::assertSame('config1', $jobDefinition->configId);
        self::assertSame('row1', $jobDefinition->rowId);
        self::assertFalse($jobDefinition->isDisabled);
        self::assertSame($configuration, $jobDefinition->configuration);
        self::assertSame($state, $jobDefinition->state);
    }
}
