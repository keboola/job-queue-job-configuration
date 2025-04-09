<?php

declare(strict_types=1);

namespace JobDefinition\Configuration\Processors;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\Processor;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\ProcessorDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\Processors;
use PHPUnit\Framework\TestCase;

class ProcessorsTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $processors = new Processors();

        self::assertSame([], $processors->before);
        self::assertSame([], $processors->after);
    }

    public function testConstructor(): void
    {
        $processors = new Processors(
            before: [
                new Processor(
                    new ProcessorDefinition('component-before'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
            after: [
                new Processor(
                    new ProcessorDefinition('component-after'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
        );

        self::assertEquals(
            [
                new Processor(
                    new ProcessorDefinition('component-before'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
            $processors->before,
        );

        self::assertEquals(
            [
                new Processor(
                    new ProcessorDefinition('component-after'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
            $processors->after,
        );
    }

    public static function provideFromArrayTestData(): iterable
    {
        yield 'empty data' => [
            'data' => [],
            'expected' => new Processors(),
        ];
        yield 'full data' => [
            'data' => [
                'before' => [
                    [
                        'definition' => [
                            'component' => 'first-component-before',
                        ],
                        'parameters' => [
                            'some-param' => 'some-value',
                        ],
                    ],
                    [
                        'definition' => [
                            'component' => 'second-component-before',
                        ],
                    ],
                ],
                'after' => [
                    [
                        'definition' => [
                            'component' => 'first-component-after',
                        ],
                    ],
                ],
            ],
            'expected' => new Processors(
                before: [
                    new Processor(
                        new ProcessorDefinition('first-component-before'),
                        [
                            'some-param' => 'some-value',
                        ],
                    ),
                    new Processor(
                        new ProcessorDefinition('second-component-before'),
                    ),
                ],
                after: [
                    new Processor(
                        new ProcessorDefinition('first-component-after'),
                    ),
                ],
            ),
        ];
    }

    /** @dataProvider provideFromArrayTestData */
    public function testFromArray(array $data, Processors $expected): void
    {
        $processors = Processors::fromArray($data);

        self::assertEquals($expected, $processors);
    }

    public function testToArray(): void
    {
        $processors = new Processors(
            before: [
                new Processor(
                    new ProcessorDefinition('component-before'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
            after: [
                new Processor(
                    new ProcessorDefinition('component-after'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
        );

        self::assertSame([
            'before' => [
                [
                    'definition' => [
                        'component' => 'component-before',
                    ],
                    'parameters' => [
                        'some-param' => 'some-value',
                    ],
                ],
            ],
            'after' => [
                [
                    'definition' => [
                        'component' => 'component-after',
                    ],
                    'parameters' => [
                        'some-param' => 'some-value',
                    ],
                ],
            ],
        ], $processors->toArray());
    }

    public function testToArrayWithNoProcessors(): void
    {
        $processors = new Processors(
            before: [],
            after: [],
        );

        self::assertSame([], $processors->toArray());
    }
}
