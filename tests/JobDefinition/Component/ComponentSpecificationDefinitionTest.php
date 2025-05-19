<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Component;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecificationDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class ComponentSpecificationDefinitionTest extends TestCase
{
    public function testConfiguration(): void
    {
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
                'memory' => '64m',
                'vendor' => ['a' => 'b'],
                'image_parameters' => ['foo' => 'bar'],
                'synchronous_actions' => ['test', 'test2'],
                'network' => 'none',
                'logging' => [
                    'type' => 'gelf',
                    'verbosity' => [200 => 'verbose'],
                    'no_application_errors' => true,
                ],
            ],
        ];
        $expectedConfiguration = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'latest',
                    'digest' => '',
                ],
                'memory' => '64m',
                'configuration_format' => 'json',
                'process_timeout' => 3600,
                'forward_token' => false,
                'forward_token_details' => false,
                'default_bucket' => false,
                'default_bucket_stage' => 'in',
                'vendor' => ['a' => 'b'],
                'image_parameters' => ['foo' => 'bar'],
                'synchronous_actions' => ['test', 'test2'],
                'network' => 'none',
                'logging' => [
                    'type' => 'gelf',
                    'verbosity' => [200 => 'verbose'],
                    'gelf_server_type' => 'tcp',
                    'no_application_errors' => true,
                ],
                'staging_storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
            'features' => [],
        ];
        $processedConfiguration =
            (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['component' => $config]);
        self::assertEquals($expectedConfiguration, $processedConfiguration);
    }

    public function testEmptyConfiguration(): void
    {
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
            ],
        ];
        $processedConfiguration =
            (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['component' => $config]);
        $expectedConfiguration = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'latest',
                    'digest' => '',
                ],
                'memory' => '256m',
                'configuration_format' => 'json',
                'process_timeout' => 3600,
                'forward_token' => false,
                'forward_token_details' => false,
                'default_bucket' => false,
                'synchronous_actions' => [],
                'default_bucket_stage' => 'in',
                'staging_storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
                'image_parameters' => [],
                'network' => 'bridge',
                'logging' => [
                    'type' => 'standard',
                    'verbosity' => [
                        100 => 'none',
                        200 => 'normal',
                        250 => 'normal',
                        300 => 'normal',
                        400 => 'normal',
                        500 => 'camouflage',
                        550 => 'camouflage',
                        600 => 'camouflage',
                    ],
                    'gelf_server_type' => 'tcp',
                    'no_application_errors' => false,
                ],
            ],
            'features' => [],
        ];
        self::assertEquals($expectedConfiguration, $processedConfiguration);
    }

    public function testWrongDefinitionType(): void
    {
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'whatever',
                    'uri' => 'keboola/docker-demo',
                ],
                'memory' => '64m',
            ],
            'features' => [],
        ];
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage(
            'Invalid configuration for path "component.data.definition.type": Invalid image type "whatever".',
        );
        (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['component' => $config]);
    }

    public function testWrongConfigurationFormat(): void
    {
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
                'memory' => '64m',
                'configuration_format' => 'fail',
            ],
            'features' => [],
        ];
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage(
            'Invalid configuration for path "component.data.configuration_format": ' .
            'Invalid configuration_format "fail".',
        );
        (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['component' => $config]);
    }

    public function testExtraConfigurationField(): void
    {
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
                'unknown' => [],
            ],
            'features' => [],
        ];
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('Unrecognized option "unknown" under "component.data"');
        (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['component' => $config]);
    }

    public function testWrongNetworkType(): void
    {
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
                'memory' => '64m',
                'network' => 'whatever',
            ],
            'features' => [],
        ];
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage(
            'Invalid configuration for path "component.data.network": Invalid network type "whatever".',
        );
        (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['component' => $config]);
    }

    public function testWrongStagingInputStorageType(): void
    {
        $this->expectException('\Symfony\Component\Config\Definition\Exception\InvalidConfigurationException');
        $this->expectExceptionMessage(
            'The value "whatever" is not allowed for path "component.data.staging_storage.input". ' .
            'Permissible values: "local", "s3", "abs", "none", "workspace-snowflake", "workspace-bigquery"',
        );
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
                'memory' => '64m',
                'staging_storage' => [
                    'input' => 'whatever',
                ],
            ],
            'features' => [],
        ];
        (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['component' => $config]);
    }

    public function testWrongStagingOutputStorageType(): void
    {
        $this->expectException('\Symfony\Component\Config\Definition\Exception\InvalidConfigurationException');
        $this->expectExceptionMessage(
            'The value "whatever" is not allowed for path "component.data.staging_storage.output". ' .
            'Permissible values: "local", "none", "workspace-snowflake", "workspace-bigquery"',
        );
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
                'memory' => '64m',
                'staging_storage' => [
                    'output' => 'whatever',
                ],
            ],
            'features' => [],
        ];
        (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['component' => $config]);
    }
}
