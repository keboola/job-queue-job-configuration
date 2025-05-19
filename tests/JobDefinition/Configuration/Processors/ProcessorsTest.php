<?php

declare(strict_types=1);

namespace JobDefinition\Configuration\Processors;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\Processor;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\ProcessorDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\Processors;
use PHPUnit\Framework\TestCase;

class ProcessorsTest extends TestCase
{
    public static function provideConstructorTestData(): iterable
    {
        yield 'empty constructor' => [
            'before' => [],
            'after' => [],
            'expectedBefore' => [],
            'expectedAfter' => [],
        ];

        yield 'with processors' => [
            'before' => [
                new Processor(
                    new ProcessorDefinition('component-before'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
            'after' => [
                new Processor(
                    new ProcessorDefinition('component-after', 'latest'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
            'expectedBefore' => [
                new Processor(
                    new ProcessorDefinition('component-before'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
            'expectedAfter' => [
                new Processor(
                    new ProcessorDefinition('component-after', 'latest'),
                    [
                        'some-param' => 'some-value',
                    ],
                ),
            ],
        ];
    }

    /**
     * @dataProvider provideConstructorTestData
     */
    public function testConstructor(
        array $before,
        array $after,
        array $expectedBefore,
        array $expectedAfter,
    ): void {
        $processors = new Processors(
            before: $before,
            after: $after,
        );

        self::assertEquals($expectedBefore, $processors->before);
        self::assertEquals($expectedAfter, $processors->after);
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
                            'tag' => 'latest',
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
                        new ProcessorDefinition('second-component-before', 'latest'),
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

    public static function provideToArrayTestData(): iterable
    {
        yield 'with processors' => [
            'processors' => new Processors(
                before: [
                    new Processor(
                        new ProcessorDefinition('component-before', null),
                        [
                            'some-param' => 'some-value',
                        ],
                    ),
                ],
                after: [
                    new Processor(
                        new ProcessorDefinition('component-after', 'latest'),
                        [
                            'some-param' => 'some-value',
                        ],
                    ),
                ],
            ),
            'expected' => [
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
                            'tag' => 'latest',
                        ],
                        'parameters' => [
                            'some-param' => 'some-value',
                        ],
                    ],
                ],
            ],
        ];

        yield 'with no processors' => [
            'processors' => new Processors(
                before: [],
                after: [],
            ),
            'expected' => [],
        ];
    }

    /**
     * @dataProvider provideToArrayTestData
     */
    public function testToArray(Processors $processors, array $expected): void
    {
        self::assertSame($expected, $processors->toArray());
    }
}
