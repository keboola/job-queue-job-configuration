<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\JobDefinition\Configuration\Authorization;

use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Authorization\AuthorizationDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class AuthorizationDefinitionTest extends TestCase
{
    public static function provideValidAuthorizationConfig(): iterable
    {
        yield 'no auth' => [
            'config' => [],
        ];

        yield 'configured oauth' => [
            'config' => [
                'oauth_api' => [
                    'id' => '123',
                    'version' => 2,
                    'credentials' => [
                        'key' => 'value',
                    ],
                ],
            ],
        ];

        yield 'configured workspace' => [
            'config' => [
                'workspace' => [
                    'container' => 'my-container',
                    'connectionString' => 'aVeryLongString',
                    'account' => 'test',
                    'region' => 'mordor',
                    'credentials' => [
                        'client_id' => 'client123',
                        'private_key' => 'very-secret-private-key',
                    ],
                ],
            ],
        ];

        yield 'configured workspace with privateKey' => [
            'config' => [
                'workspace' => [
                    'host' => 'host-value',
                    'account' => 'account-value',
                    'warehouse' => 'warehouse-value',
                    'database' => 'database-value',
                    'schema' => 'schema-value',
                    'region' => 'region-value',
                    'privateKey' => 'private-key-value',
                ],
            ],
        ];

        yield 'configured GitHub App token' => [
            'config' => [
                'githubApp' => [
                    '#token' => 'KBC::ProjectSecure::token',
                    'expiresAt' => '2026-10-12T13:00:00+00:00',
                ],
            ],
        ];

        yield 'configured context' => [
            'config' => [
                'context' => 'wlm',
            ],
        ];

        yield 'configured app proxy' => [
            'config' => [
                'app_proxy' => [
                    'auth_providers' => [],
                    'auth_rules' => [
                        [
                            'type' => 'pathPrefix',
                            'value' => '/',
                            'auth_required' => false,
                        ],
                    ],
                ],
            ],
        ];
    }

    /** @dataProvider provideValidAuthorizationConfig */
    public function testValidAuthorizationConfig(?array $config): void
    {
        (new Processor())->processConfiguration(new AuthorizationDefinition(), [
            'authorization' => $config,
        ]);
        self::assertTrue(true);
    }

    public static function provideInvalidAuthorizationConfig(): iterable
    {
        yield 'GitHub App without token' => [
            'config' => [
                'githubApp' => [
                    'expiresAt' => '2026-10-12T13:00:00+00:00',
                ],
            ],
            'expectedMessage' => 'The child config "#token" under "authorization.githubApp" must be configured.',
        ];

        yield 'GitHub App with empty token' => [
            'config' => [
                'githubApp' => [
                    '#token' => '',
                    'expiresAt' => '2026-10-12T13:00:00+00:00',
                ],
            ],
            'expectedMessage' => 'The path "authorization.githubApp.#token" cannot contain an empty value, but got "".',
        ];

        yield 'GitHub App without expiration' => [
            'config' => [
                'githubApp' => [
                    '#token' => 'KBC::ProjectSecure::token',
                ],
            ],
            'expectedMessage' => 'The child config "expiresAt" under "authorization.githubApp" must be configured.',
        ];
    }

    /** @dataProvider provideInvalidAuthorizationConfig */
    public function testInvalidAuthorizationConfig(array $config, string $expectedMessage): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedMessage);

        (new Processor())->processConfiguration(new AuthorizationDefinition(), [
            'authorization' => $config,
        ]);
    }
}
