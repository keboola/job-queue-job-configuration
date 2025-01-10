<?php

declare(strict_types=1);

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../vendor/autoload.php';

$envPath = dirname(__DIR__) . '/.env';
if (file_exists($envPath)) {
    (new Dotenv())->usePutenv()->bootEnv(
        path: $envPath,
        testEnvs: [],
        overrideExistingVars: true,
    );
}

$stackUrlToTokensEnv = [
    'STORAGE_API_URL' => [
        'TEST_STORAGE_API_TOKEN',
        'TEST_STORAGE_API_TOKEN_MASTER',
        'TEST_STORAGE_API_TOKEN_FEATURE_NEW_NATIVE_TYPES',
        'TEST_STORAGE_API_TOKEN_FEATURE_NATIVE_TYPES',
    ],
    'STORAGE_API_URL_GCP' => [
        'TEST_STORAGE_API_TOKEN_GCP',
        'TEST_STORAGE_API_TOKEN_MASTER_GCP',
    ],
    'STORAGE_API_URL_AZURE' => [
        'TEST_STORAGE_API_TOKEN_AZURE',
        'TEST_STORAGE_API_TOKEN_MASTER_AZURE',
    ],
    'STORAGE_API_URL__REDSHIFT' => [
        'TEST_STORAGE_API_TOKEN_REDSHIFT',
        'TEST_STORAGE_API_TOKEN_MASTER_REDSHIFT',
    ],
];

foreach ($stackUrlToTokensEnv as $stackUrlEnv => $tokensEnv) {
    if (getenv($stackUrlEnv) === false || getenv($stackUrlEnv) === '') {
        // We do not want to fail if the stack is not set in CI
        continue;
    }

    foreach ($tokensEnv as $tokenEnv) {
        verifyToken($stackUrlEnv, $tokenEnv);
    }
}

function verifyToken(string $storageApiUrlEnv, string $storageApiTokenEnv): void
{
    $client = new Client([
        'url' => getenv($storageApiUrlEnv),
        'token' => getenv($storageApiTokenEnv),
    ]);

    try {
        $tokenInfo = $client->verifyToken();
    } catch (ClientException $e) {
        throw new RuntimeException(sprintf(
            'Failed to verify "%s" (%s) against %s, check ENV variables: %s',
            $storageApiTokenEnv,
            getenv($storageApiTokenEnv) ? (substr((string) getenv($storageApiTokenEnv), 0, 10) . '...') : 'empty',
            getenv($storageApiUrlEnv),
            $e->getMessage(),
        ), 0, $e);
    }

    printf(
        'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.' . PHP_EOL,
        $tokenInfo['description'],
        $tokenInfo['id'],
        $tokenInfo['owner']['name'],
        $tokenInfo['owner']['id'],
        $client->getApiUrl(),
    );
}
