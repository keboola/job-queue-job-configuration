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

    public function testFromArray(): void
    {
        $definition = ProcessorDefinition::fromArray([
            'component' => 'test-component',
        ]);

        self::assertSame('test-component', $definition->component);
    }

    public function testToArray(): void
    {
        $definition = new ProcessorDefinition('test-component');

        self::assertSame([
            'component' => 'test-component',
        ], $definition->toArray());
    }
}
