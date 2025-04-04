<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Processors;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\ProcessorDefinition;
use PHPUnit\Framework\TestCase;

class ProcessorDefinitionTest extends TestCase
{
    public function testConstructor(): void
    {
        $definition = new ProcessorDefinition('test-component');

        self::assertSame('test-component', $definition->component);
    }

    public static function provideFromArrayTestData(): iterable
    {
        yield 'empty data' => [
            'data' => [],
            'expectedComponent' => '',
        ];
        yield 'defined component' => [
            'data' => [
                'component' => 'test-component',
            ],
            'expectedComponent' => 'test-component',
        ];
    }

    /** @dataProvider provideFromArrayTestData */
    public function testFromArray(array $data, string $expectedComponent): void
    {
        $definition = ProcessorDefinition::fromArray($data);
        self::assertSame($expectedComponent, $definition->component);
    }

    public function testToArray(): void
    {
        $definition = new ProcessorDefinition('test-component');

        self::assertSame([
            'component' => 'test-component',
        ], $definition->toArray());
    }
}
