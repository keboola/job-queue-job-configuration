<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\Component;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\JobDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use PHPUnit\Framework\TestCase;

class JobDefinitionTest extends TestCase
{
    public function testConstruct(): void
    {
        $component = new Component([
            'id' => 'my.component',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
            ],
        ]);

        $configuration = Configuration::fromArray([]);
        $state = State::fromArray([]);

        $jobDefinition = new JobDefinition(
            component: $component,
            configId: 'configId',
            rowId: 'rowId',
            isDisabled: true,
            configuration: $configuration,
            state: $state,
        );
        self::assertSame($component, $jobDefinition->component);
        self::assertSame('configId', $jobDefinition->configId);
        self::assertSame('rowId', $jobDefinition->rowId);
        self::assertTrue($jobDefinition->isDisabled);
        self::assertSame($configuration, $jobDefinition->configuration);
        self::assertSame($state, $jobDefinition->state);
    }
}
