<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition;

use InvalidArgumentException;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Configuration;
use Keboola\JobQueue\JobConfiguration\JobDefinition\JobDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\State\State;
use PHPUnit\Framework\TestCase;

class JobDefinitionTest extends TestCase
{
    public static function provideValidConfigIdAndVersionTestData(): iterable
    {
        yield 'both configId & configVersion set' => [
            'configId' => '123',
            'configVersion' => '1',
        ];
        yield 'configId set, configVersion empty' => [
            'configId' => '123',
            'configVersion' => null,
        ];
        yield 'both configId & configVersion empty' => [
            'configId' => '',
            'configVersion' => null,
        ];
    }

    /** @dataProvider provideValidConfigIdAndVersionTestData */
    public function testConstruct(
        ?string $configId,
        ?string $configVersion,
    ): void {
        $component = new ComponentSpecification([
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
            configId: $configId,
            configVersion: $configVersion,
            rowId: 'rowId',
            isDisabled: true,
            configuration: $configuration,
            state: $state,
        );
        self::assertSame($component, $jobDefinition->component);
        self::assertSame($configId, $jobDefinition->configId);
        self::assertSame($configVersion, $jobDefinition->configVersion);
        self::assertSame('rowId', $jobDefinition->rowId);
        self::assertTrue($jobDefinition->isDisabled);
        self::assertSame($configuration, $jobDefinition->configuration);
        self::assertSame($state, $jobDefinition->state);
    }

    public static function provideInvalidConfigIdAndVersionTestData(): iterable
    {
        yield 'configId empty, configVersion set' => [
            'configId' => '',
            'configVersion' => '1',
        ];
        yield 'configId null, configVersion set' => [
            'configId' => null,
            'configVersion' => '1',
        ];
    }

    /** @dataProvider provideInvalidConfigIdAndVersionTestData */
    public function testFailsIfConfigIdIsEmptyAndVersionIsSet(
        ?string $configId,
        ?string $configVersion,
    ): void {
        $component = new ComponentSpecification([
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

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('configVersion cannot be set if configId is empty.');

        new JobDefinition(
            component: $component,
            configId: $configId,
            configVersion: $configVersion,
            rowId: 'rowId',
            isDisabled: true,
            configuration: $configuration,
            state: $state,
        );
    }
}
