<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Component;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\AllowedProcessorPosition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecification;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Component\ComponentSpecificationDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\DataTypeSupport;
use Keboola\JobQueue\JobConfiguration\JobDefinition\UnitConverter;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class ComponentSpecificationTest extends TestCase
{
    public function testConfiguration(): void
    {
        $configuration = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
                'memory' => '128m',
                'process_timeout' => 7200,
                'forward_token' => true,
                'forward_token_details' => true,
                'default_bucket' => true,
            ],
            'dataTypesConfiguration' => [
                'dataTypesSupport' => 'authoritative',
            ],
            'processorConfiguration' => [
                'allowedProcessorPosition' => 'before',
            ],
        ];

        $component = new ComponentSpecification($configuration);
        self::assertSame('128m', $component->getMemoryLimit());
        self::assertSame(UnitConverter::connectionMemoryLimitToBytes('128m'), $component->getMemoryLimitBytes());
        self::assertSame(7200, $component->getProcessTimeout());
        self::assertSame('standard', $component->getLoggerType());
        self::assertSame(
            [
                100 => 'none',
                200 => 'normal',
                250 => 'normal',
                300 => 'normal',
                400 => 'normal',
                500 => 'camouflage',
                550 => 'camouflage',
                600 => 'camouflage',
            ],
            $component->getLoggerVerbosity(),
        );
        self::assertTrue($component->hasForwardToken());
        self::assertTrue($component->hasForwardTokenDetails());
        self::assertTrue($component->hasDefaultBucket());
        self::assertSame('keboola/docker-demo', $component->getImageUri());
        self::assertSame('master', $component->getImageTag());
        self::assertSame(DataTypeSupport::AUTHORITATIVE, $component->getDataTypesSupport());
        self::assertSame(AllowedProcessorPosition::BEFORE, $component->getAllowedProcessorPosition());
    }

    public function testConfigurationDefaults(): void
    {
        $configuration = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                    'tag' => 'master',
                ],
            ],
        ];

        $component = new ComponentSpecification($configuration);
        self::assertSame('256m', $component->getMemoryLimit());
        self::assertSame(UnitConverter::connectionMemoryLimitToBytes('256m'), $component->getMemoryLimitBytes());
        self::assertSame(3600, $component->getProcessTimeout());
        self::assertSame('standard', $component->getLoggerType());
        self::assertSame(
            [
                100 => 'none',
                200 => 'normal',
                250 => 'normal',
                300 => 'normal',
                400 => 'normal',
                500 => 'camouflage',
                550 => 'camouflage',
                600 => 'camouflage',
            ],
            $component->getLoggerVerbosity(),
        );
        self::assertFalse($component->hasForwardToken());
        self::assertFalse($component->hasForwardTokenDetails());
        self::assertFalse($component->hasDefaultBucket());
    }

    public function testInvalidComponentNoDefinition(): void
    {
        self::expectException(ApplicationExceptionInterface::class);
        self::expectExceptionMessage(
            'Component definition is invalid. Verify the deployment setup and the repository settings in ' .
            'the Developer Portal. Detail: The child config "definition" under "component.data" must be configured.',
        );
        new ComponentSpecification([]);
    }

    public function testInvalidComponentEmptyDefinition(): void
    {
        self::expectException(ApplicationExceptionInterface::class);
        self::expectExceptionMessage(
            'Component definition is invalid. Verify the deployment setup and the repository settings in the ' .
            'Developer Portal. Detail: The child config "definition" under "component.data" must be configured',
        );
        new ComponentSpecification([
            'data' => [
            ],
        ]);
    }

    public function testInvalidComponentEmptyUri(): void
    {
        self::expectException(ApplicationExceptionInterface::class);
        self::expectExceptionMessage(
            'Component definition is invalid. Verify the deployment setup and the repository settings in the ' .
            'Developer Portal. Detail: The path "component.data.definition.uri" cannot contain an empty value, ' .
            'but got "".',
        );
        new ComponentSpecification([
            'data' => [
                'definition' => [
                    'type' => 'aws-ecr',
                    'uri' => '',
                ],
            ],
        ]);
    }

    public function testGetSanitizedBucketNameDot(): void
    {
        $component = [
            'id' => 'keboola.ex-generic',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'dummy',
                ],
            ],
        ];
        $component = new ComponentSpecification($component);
        self::assertEquals('in.c-keboola-ex-generic-test', $component->getDefaultBucketName('test'));
    }

    public function testGetSanitizedBucketNameNoDot(): void
    {
        $component = [
            'id' => 'ex-generic',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'dummy',
                ],
            ],
        ];
        $component = new ComponentSpecification($component);
        self::assertEquals('in.c-ex-generic-test', $component->getDefaultBucketName('test'));
    }

    public function testGetSanitizedBucketNameTwoDot(): void
    {
        $component = [
            'id' => 'keboola.ex.generic',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'dummy',
                ],
            ],
        ];
        $component = new ComponentSpecification($component);
        self::assertEquals('in.c-keboola-ex-generic-test', $component->getDefaultBucketName('test'));
    }

    public function testFlagsOff(): void
    {
        $componentData = [
            'id' => 'keboola.ex-generic',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'dummy',
                ],
            ],
        ];
        $component = new ComponentSpecification($componentData);
        self::assertFalse($component->runAsRoot());
        self::assertFalse($component->allowBranchMapping());
        self::assertFalse($component->blockBranchJobs());
        self::assertFalse($component->branchConfigurationsAreUnsafe());
    }

    public function testFlagsOn(): void
    {
        $componentData = [
            'id' => 'keboola.ex-generic',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'dummy',
                ],
            ],
            'features' => [
                'container-root-user',
                'dev-branch-configuration-unsafe',
                'dev-branch-job-blocked',
                'dev-mapping-allowed',
                'container-tcpkeepalive-60s-override',
            ],
        ];
        $component = new ComponentSpecification($componentData);
        self::assertTrue($component->runAsRoot());
        self::assertTrue($component->allowBranchMapping());
        self::assertTrue($component->blockBranchJobs());
        self::assertTrue($component->branchConfigurationsAreUnsafe());
        self::assertTrue($component->overrideKeepalive60s());
    }

    public function testHasSwap(): void
    {
        $componentData = [
            'id' => 'keboola.ex-generic',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'dummy',
                ],
            ],
        ];
        $component = new ComponentSpecification($componentData);
        self::assertFalse($component->hasNoSwap());
    }

    public function testHasNoSwap(): void
    {
        $componentData = [
            'id' => 'keboola.ex-generic',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'dummy',
                ],
            ],
            'features' => [
                'no-swap',
            ],
        ];
        $component = new ComponentSpecification($componentData);
        self::assertTrue($component->hasNoSwap());
    }

    public function testSetTag(): void
    {
        $componentData = [
            'id' => 'keboola.ex-generic',
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'dummy',
                ],
            ],
            'features' => [
                'no-swap',
            ],
        ];
        $component = new ComponentSpecification($componentData);
        $component->setImageTag('1.2.3');
        self::assertSame('1.2.3', $component->getImageTag());
    }

    /** @dataProvider provideGetImageUriWithTagTestData */
    public function testGetImageUriWithTag(
        array $componentDefinition,
        ?string $customTag,
        string $expectedImageUri,
    ): void {
        $componentSpec = new ComponentSpecification([
            'id' => 'keboola.test-component',
            'data' => [
                'definition' => $componentDefinition,
            ],
        ]);
        $imageUri = $componentSpec->getImageUriWithTag($customTag);

        self::assertSame($expectedImageUri, $imageUri);
    }

    public static function provideGetImageUriWithTagTestData(): iterable
    {
        yield 'default tag' => [
            'componentDefinition' => [
                'type' => 'aws-ecr',
                'uri' => 'keboola/test-component',
                'tag' => '0.2.2',
            ],
            'customTag' => null,
            'expectedImageUri' => 'keboola/test-component:0.2.2',
        ];

        yield 'custom tag' => [
            'componentDefinition' => [
                'type' => 'aws-ecr',
                'uri' => 'keboola/test-component',
                'tag' => '0.2.2',
            ],
            'customTag' => '0.3.3',
            'expectedImageUri' => 'keboola/test-component:0.3.3',
        ];
    }

    /** @dataProvider provideGetSynchronousActionTestData */
    public function testGetSynchronousAction(
        array $componentData,
        array $expectedResult,
    ): void {
        $componentSpec = new ComponentSpecification([
            'id' => 'keboola.test-component',
            'data' => array_merge([
                'definition' => [
                    'type' => 'aws-ecr',
                    'uri' => 'keboola/test-component',
                    'tag' => '0.2.2',
                ],
            ], $componentData),
        ]);
        $result = $componentSpec->getSynchronousActions();

        self::assertSame($expectedResult, $result);
    }

    public static function provideGetSynchronousActionTestData(): iterable
    {
        yield 'no actions configured' => [
            'componentData' => [],
            'result' => [],
        ];

        yield 'empty actions configured' => [
            'componentData' => [
                'synchronous_actions' => [],
            ],
            'result' => [],
        ];

        yield 'existing action' => [
            'componentData' => [
                'synchronous_actions' => [
                    'my-action',
                    'other-action',
                ],
            ],
            'result' => [
                'my-action',
                'other-action',
            ],
        ];
    }

    /** @dataProvider provideHasSynchronousActionTestData */
    public function testHasSynchronousAction(
        array $componentData,
        string $action,
        bool $expectedResult,
    ): void {
        $componentSpec = new ComponentSpecification([
            'id' => 'keboola.test-component',
            'data' => array_merge([
                'definition' => [
                    'type' => 'aws-ecr',
                    'uri' => 'keboola/test-component',
                    'tag' => '0.2.2',
                ],
            ], $componentData),
        ]);
        $result = $componentSpec->hasSynchronousAction($action);

        self::assertSame($expectedResult, $result);
    }

    public static function provideHasSynchronousActionTestData(): iterable
    {
        yield 'no actions configured' => [
            'componentData' => [],
            'action' => 'my-action',
            'result' => false,
        ];

        yield 'empty actions configured' => [
            'componentData' => [
                'synchronous_actions' => [],
            ],
            'action' => 'my-action',
            'result' => false,
        ];

        yield 'non-existing action' => [
            'componentData' => [
                'synchronous_actions' => [
                    'other-action',
                ],
            ],
            'action' => 'my-action',
            'result' => false,
        ];

        yield 'existing action' => [
            'componentData' => [
                'synchronous_actions' => [
                    'other-action',
                    'my-action',
                ],
            ],
            'action' => 'my-action',
            'result' => true,
        ];
    }

    public function testWrongDataTypesSupportValue(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The value "whatever" is not allowed for path "component.dataTypesConfiguration.dataTypesSupport". ' .
            'Permissible values: "authoritative", "hints", "none"',
        );
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
                'memory' => '64m',
            ],
            'dataTypesConfiguration' => [
                'dataTypesSupport' => 'whatever',
            ],
        ];
        (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['config' => $config]);
    }

    public function testWrongAllowedProcessorPositionValue(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'The value "whatever" is not allowed for path "component.processorConfiguration.allowedProcessorPosition".'.
            ' Permissible values: "any", "before", "after"',
        );
        $config = [
            'data' => [
                'definition' => [
                    'type' => 'dockerhub',
                    'uri' => 'keboola/docker-demo',
                ],
                'memory' => '64m',
            ],
            'processorConfiguration' => [
                'allowedProcessorPosition' => 'whatever',
            ],
        ];
        (new Processor())->processConfiguration(new ComponentSpecificationDefinition(), ['config' => $config]);
    }
}
