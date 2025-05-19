<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Processors;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Processors\ProcessorDefinition;
use PHPUnit\Framework\TestCase;

class ProcessorDefinitionTest extends TestCase
{
    public static function provideConstructorTestData(): iterable
    {
        yield 'with explicit null tag' => [
            'component' => 'test-component',
            'tag' => null,
            'expectedComponent' => 'test-component',
            'expectedTag' => null,
        ];

        yield 'with tag' => [
            'component' => 'test-component',
            'tag' => 'latest',
            'expectedComponent' => 'test-component',
            'expectedTag' => 'latest',
        ];

        yield 'without tag parameter' => [
            'component' => 'test-component',
            'tag' => null,
            'expectedComponent' => 'test-component',
            'expectedTag' => null,
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
        string $expectedComponent,
        ?string $expectedTag,
        bool $skipTagParameter = false,
    ): void {
        if ($skipTagParameter) {
            $definition = new ProcessorDefinition($component);
        } else {
            $definition = new ProcessorDefinition($component, $tag);
        }

        self::assertSame($expectedComponent, $definition->component);
        self::assertSame($expectedTag, $definition->tag);
    }

    public static function provideFromArrayTestData(): iterable
    {
        yield 'without tag' => [
            'data' => [
                'component' => 'test-component',
            ],
            'expectedComponent' => 'test-component',
            'expectedTag' => null,
        ];

        yield 'with tag' => [
            'data' => [
                'component' => 'test-component',
                'tag' => 'latest',
            ],
            'expectedComponent' => 'test-component',
            'expectedTag' => 'latest',
        ];

        yield 'with empty tag' => [
            'data' => [
                'component' => 'test-component',
                'tag' => '',
            ],
            'expectedComponent' => 'test-component',
            'expectedTag' => null,
        ];
    }

    /**
     * @param array{
     *     component: non-empty-string,
     *     tag?: ?string,
     * } $data
     * @param non-empty-string $expectedComponent
     * @param null|non-empty-string $expectedTag
     * @dataProvider provideFromArrayTestData
     */
    public function testFromArray(
        array $data,
        string $expectedComponent,
        ?string $expectedTag,
    ): void {
        $definition = ProcessorDefinition::fromArray($data);

        self::assertSame($expectedComponent, $definition->component);
        self::assertSame($expectedTag, $definition->tag);
    }

    public static function provideToArrayTestData(): iterable
    {
        yield 'without tag' => [
            'component' => 'test-component',
            'tag' => null,
            'expected' => [
                'component' => 'test-component',
            ],
        ];

        yield 'with tag' => [
            'component' => 'test-component',
            'tag' => 'latest',
            'expected' => [
                'component' => 'test-component',
                'tag' => 'latest',
            ],
        ];
    }

    /**
     * @param non-empty-string $component
     * @param null|non-empty-string $tag
     * @dataProvider provideToArrayTestData
     */
    public function testToArray(
        string $component,
        ?string $tag,
        array $expected,
    ): void {
        $definition = new ProcessorDefinition($component, $tag);

        self::assertSame($expected, $definition->toArray());
    }
}
