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

    public static function provideFromArrayTestData(): iterable
    {
        yield 'empty data' => [
            'data' => [],
            'expected' => [
                'definition' => new ProcessorDefinition(''),
                'parameters' => [],
             ],
        ];
        yield 'component & parameters defined' => [
            'data' => [
                'definition' => [
                    'component' => 'test-component',
                ],
                'parameters' => [
                    'some-param' => 'some-value',
                ],
            ],
            'expected' => [
                'definition' => new ProcessorDefinition('test-component'),
                'parameters' => [
                    'some-param' => 'some-value',
                ],
            ],
        ];
    }

    /** @dataProvider provideFromArrayTestData */
    public function testFromArray(array $data, array $expected): void
    {
        $processor = Processor::fromArray($data);

        self::assertEquals($expected['definition'], $processor->definition);
        self::assertSame($expected['parameters'], $processor->parameters);
    }

    public static function provideToArrayTestData(): iterable
    {
        yield 'empty component & params' => [
            'processor' => new Processor(
                new ProcessorDefinition(''),
                [],
            ),
            'expected' => [
                'definition' => [
                    'component' => '',
                ],
            ],
        ];
        yield 'defined component & params' => [
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
