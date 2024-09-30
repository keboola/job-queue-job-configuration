<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../vendor/autoload.php';

$dotenv = (new Dotenv())->usePutenv();

$envLocalPath = dirname(__DIR__) . '/.env.local';
if (!file_exists($envLocalPath)) {
    throw new RuntimeException('.env.local file not found.');
}

$dotenv->bootEnv(
    path: $envLocalPath,
    testEnvs: [],
    overrideExistingVars: true
);
