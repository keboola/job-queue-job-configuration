<?php

declare(strict_types=1);

namespace JobDefinition\Configuration\Processors;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\Processor;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\ProcessorDefinition;
use PHPUnit\Framework\TestCase;

class ProcessorTest extends TestCase
{
    public static function provideConstructorTestData(): iterable
    {
        yield 'with explicit null tag' => [
            'component' => 'test-component',
            'tag' => null,
            'parameters' => [
                'some-param' => 'some-value',
            ],
            'expectedDefinition' => new ProcessorDefinition('test-component', null),
            'expectedParameters' => [
                'some-param' => 'some-value',
            ],
        ];

        yield 'with tag' => [
            'component' => 'test-component',
            'tag' => 'latest',
            'parameters' => [
                'some-param' => 'some-value',
            ],
            'expectedDefinition' => new ProcessorDefinition('test-component', 'latest'),
            'expectedParameters' => [
                'some-param' => 'some-value',
            ],
        ];

        yield 'without tag parameter' => [
            'component' => 'test-component',
            'tag' => null,
            'parameters' => [
                'some-param' => 'some-value',
            ],
            'expectedDefinition' => new ProcessorDefinition('test-component'),
            'expectedParameters' => [
                'some-param' => 'some-value',
            ],
            'skipTagParameter' => true,
        ];
    }

    /**
     * @param non-empty-string $component
     * @param null|non-empty-string $tag
     * @dataProvider provideConstructorTestData
     */
    public function testConstructor(
        string $component,
        ?string $tag,
        array $parameters,
        ProcessorDefinition $expectedDefinition,
        array $expectedParameters,
        bool $skipTagParameter = false,
    ): void {
        if ($skipTagParameter) {
            $definition = new ProcessorDefinition($component);
            $processor = new Processor($definition, $parameters);
        } else {
            $definition = new ProcessorDefinition($component, $tag);
            $processor = new Processor($definition, $parameters);
        }

        self::assertEquals($expectedDefinition, $processor->definition);
        self::assertSame($expectedParameters, $processor->parameters);
    }

    public static function provideConstructorWithUndefinedProcessorParametersTestData(): iterable
    {
        yield 'with explicit null tag' => [
            'component' => 'test-component',
            'tag' => null,
            'expectedDefinition' => new ProcessorDefinition('test-component', null),
            'expectedParameters' => [],
        ];

        yield 'with tag' => [
            'component' => 'test-component',
            'tag' => 'latest',
            'expectedDefinition' => new ProcessorDefinition('test-component', 'latest'),
            'expectedParameters' => [],
        ];

        yield 'without tag parameter' => [
            'component' => 'test-component',
            'tag' => null,
            'expectedDefinition' => new ProcessorDefinition('test-component'),
            'expectedParameters' => [],
            'skipTagParameter' => true,
        ];
    }

    /**
     * @param non-empty-string $component
     * @param null|non-empty-string $tag
     * @dataProvider provideConstructorWithUndefinedProcessorParametersTestData
     */
    public function testConstructorWithUndefinedProcessorParameters(
        string $component,
        ?string $tag,
        ProcessorDefinition $expectedDefinition,
        array $expectedParameters,
        bool $skipTagParameter = false,
    ): void {
        if ($skipTagParameter) {
            $definition = new ProcessorDefinition($component);
            $processor = new Processor($definition);
        } else {
            $definition = new ProcessorDefinition($component, $tag);
            $processor = new Processor($definition);
        }

        self::assertEquals($expectedDefinition, $processor->definition);
        self::assertSame($expectedParameters, $processor->parameters);
    }

    public static function provideFromArrayTestData(): iterable
    {
        yield 'without tag' => [
            'data' => [
                'definition' => [
                    'component' => 'test-component',
                ],
                'parameters' => [
                    'some-param' => 'some-value',
                ],
            ],
            'expectedDefinition' => new ProcessorDefinition('test-component', null),
            'expectedParameters' => [
                'some-param' => 'some-value',
            ],
        ];

        yield 'with tag' => [
            'data' => [
                'definition' => [
                    'component' => 'test-component',
                    'tag' => 'latest',
                ],
                'parameters' => [
                    'some-param' => 'some-value',
                ],
            ],
            'expectedDefinition' => new ProcessorDefinition('test-component', 'latest'),
            'expectedParameters' => [
                'some-param' => 'some-value',
            ],
        ];
    }

    /**
     * @dataProvider provideFromArrayTestData
     */
    public function testFromArray(
        array $data,
        ProcessorDefinition $expectedDefinition,
        array $expectedParameters,
    ): void {
        $processor = Processor::fromArray($data);

        self::assertEquals($expectedDefinition, $processor->definition);
        self::assertSame($expectedParameters, $processor->parameters);
    }

    public static function provideToArrayTestData(): iterable
    {
        yield 'empty params' => [
            'processor' => new Processor(
                new ProcessorDefinition('test-component', null),
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
                new ProcessorDefinition('test-component', null),
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
        yield 'with tag' => [
            'processor' => new Processor(
                new ProcessorDefinition('test-component', 'latest'),
                [
                    'some-param' => 'some-value',
                ],
            ),
            'expected' => [
                'definition' => [
                    'component' => 'test-component',
                    'tag' => 'latest',
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
