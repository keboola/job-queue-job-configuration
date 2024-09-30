<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require __DIR__ . '/../vendor/autoload.php';

$dotenv = (new Dotenv())->usePutenv();

$envPath = dirname(__DIR__) . '/.env';
if (!file_exists($envPath)) {
    return;
}

$dotenv->bootEnv(
    path: $envPath,
    testEnvs: [],
    overrideExistingVars: true,
);
