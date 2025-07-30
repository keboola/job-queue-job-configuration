<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Artifacts;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Artifacts;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Custom;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\CustomFilter;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Options;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Runs;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\RunsFilter;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Shared;
use PHPUnit\Framework\TestCase;

class ArtifactsTest extends TestCase
{
    public function testEmptyConstructor(): void
    {
        $artifacts = new Artifacts();

        self::assertEquals(new Options(), $artifacts->options);
        self::assertEquals(new Runs(), $artifacts->runs);
        self::assertEquals(new Custom(), $artifacts->custom);
        self::assertEquals(new Shared(), $artifacts->shared);
    }

    public function testFromEmptyArray(): void
    {
        $artifacts = Artifacts::fromArray([]);

        self::assertEquals(new Options(), $artifacts->options);
        self::assertEquals(new Runs(), $artifacts->runs);
        self::assertEquals(new Custom(), $artifacts->custom);
        self::assertEquals(new Shared(), $artifacts->shared);
    }

    public function testFromArray(): void
    {
        $data = [
            'options' => [
                'zip' => false,
            ],
            'runs' => [
                'enabled' => true,
                'filter' => [
                    'date_since' => '2023-01-01',
                    'limit' => 100,
                ],
            ],
            'custom' => [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.ex-db',
                    'config_id' => '123',
                    'branch_id' => '456',
                    'date_since' => '2023-01-01',
                    'limit' => 50,
                ],
            ],
            'shared' => [
                'enabled' => true,
            ],
        ];

        $artifacts = Artifacts::fromArray($data);

        self::assertEquals(Options::fromArray($data['options']), $artifacts->options);
        self::assertEquals(Runs::fromArray($data['runs']), $artifacts->runs);
        self::assertEquals(Custom::fromArray($data['custom']), $artifacts->custom);
        self::assertEquals(Shared::fromArray($data['shared']), $artifacts->shared);
    }

    public function testToArray(): void
    {
        $artifacts = new Artifacts();

        $expected = [
            'options' => ['zip' => true],
            'runs' => ['enabled' => false],
            'custom' => ['enabled' => false],
            'shared' => ['enabled' => false],
        ];

        self::assertSame($expected, $artifacts->toArray());
    }

    public function testToArrayWithCompleteConfiguration(): void
    {
        $artifacts = new Artifacts(
            options: new Options(zip: false),
            runs: new Runs(
                enabled: true,
                filter: new RunsFilter(dateSince: '2023-01-01', limit: 100),
            ),
            custom: new Custom(
                enabled: true,
                filter: new CustomFilter(
                    componentId: 'keboola.ex-db',
                    configId: '123',
                    branchId: '456',
                    dateSince: '2023-01-01',
                    limit: 50,
                ),
            ),
            shared: new Shared(enabled: true),
        );

        $expected = [
            'options' => ['zip' => false],
            'runs' => [
                'enabled' => true,
                'filter' => [
                    'date_since' => '2023-01-01',
                    'limit' => 100,
                ],
            ],
            'custom' => [
                'enabled' => true,
                'filter' => [
                    'component_id' => 'keboola.ex-db',
                    'config_id' => '123',
                    'branch_id' => '456',
                    'date_since' => '2023-01-01',
                    'limit' => 50,
                ],
            ],
            'shared' => ['enabled' => true],
        ];

        self::assertSame($expected, $artifacts->toArray());
    }
}
