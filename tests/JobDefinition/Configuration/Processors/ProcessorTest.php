<?php

declare(strict_types=1);

namespace JobDefinition\Configuration\Processors;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\Processor;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\ProcessorDefinition;
use PHPUnit\Framework\TestCase;

class ProcessorTest extends TestCase
{
    public function testConstructor(): void
    {
        $processor = new Processor(
            new ProcessorDefinition('test-component'),
            [
                'some-param' => 'some-value',
            ],
        );

        self::assertEquals(new ProcessorDefinition('test-component'), $processor->definition);
        self::assertSame([
            'some-param' => 'some-value',
        ], $processor->parameters);
    }

    public function testConstructorWithUndefinedProcessorParameters(): void
    {
        $processor = new Processor(
            new ProcessorDefinition('test-component'),
        );

        self::assertEquals(new ProcessorDefinition('test-component'), $processor->definition);
        self::assertSame([], $processor->parameters);
    }

    public function testFromArray(): void
    {
        $processor = Processor::fromArray([
            'definition' => [
                'component' => 'test-component',
            ],
            'parameters' => [
                'some-param' => 'some-value',
            ],
        ]);

        self::assertEquals(new ProcessorDefinition('test-component'), $processor->definition);
        self::assertSame([
            'some-param' => 'some-value',
        ], $processor->parameters);
    }

    public static function provideToArrayTestData(): iterable
    {
        yield 'empty params' => [
            'processor' => new Processor(
                new ProcessorDefinition('test-component'),
                [],
            ),
            'expected' => [
                'definition' => [
                    'component' => 'test-component',
                ],
            ],
        ];
        yield 'defined params' => [
            'processor' => new Processor(
                new ProcessorDefinition('test-component'),
                [
                    'some-param' => 'some-value',
                ],
            ),
            'expected' => [
                'definition' => [
                    'component' => 'test-component',
                ],
                'parameters' => [
                    'some-param' => 'some-value',
                ],
            ],
        ];
    }

    /** @dataProvider provideToArrayTestData */
    public function testToArray(Processor $processor, array $expected): void
    {
        self::assertSame($expected, $processor->toArray());
    }
}
