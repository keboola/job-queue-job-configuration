<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Tracing;

use Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext;
use Keboola\JobQueue\Telemetry\TracingBackend;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process;

class StepTraceContextOpenTelemetryTest extends TestCase
{
    private const EXPORT_SCRIPT = <<<'PHP'
        $span = OpenTelemetry\API\Globals::tracerProvider()->getTracer('test')->spanBuilder('job.step')->startSpan();
        $scope = $span->activate();
        (new Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext($argv[1]))->export();
        echo json_encode(['traceId' => $span->getContext()->getTraceId(), 'spanId' => $span->getContext()->getSpanId()]);
        $scope->detach();
        $span->end();
        PHP;

    private const IMPORT_SCRIPT = <<<'PHP'
        (new Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext($argv[1]))->import();
        $root = OpenTelemetry\API\Trace\LocalRootSpan::current();
        assert($root instanceof OpenTelemetry\SDK\Trace\ReadableSpanInterface);
        $parent = $root->getParentContext();
        echo json_encode([
            'name' => $root->getName(),
            'traceId' => $root->getContext()->getTraceId(),
            'parentId' => $parent->isValid() ? $parent->getSpanId() : null,
        ]);
        PHP;

    private string $dataDirPath;

    protected function setUp(): void
    {
        parent::setUp();

        if (!TracingBackend::isOpenTelemetry()) {
            self::markTestSkipped('opentelemetry extension is not loaded');
        }

        $this->dataDirPath = (new Temp())->getTmpFolder();
    }

    public function testExportWritesW3cTraceContextOfActiveSpan(): void
    {
        $exporter = $this->runPhp(self::EXPORT_SCRIPT);

        $headers = json_decode(
            (string) file_get_contents($this->dataDirPath . '/' . StepTraceContext::FILE_NAME),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        self::assertIsArray($headers);
        self::assertIsString($exporter['traceId']);
        self::assertIsString($exporter['spanId']);
        self::assertIsString($headers['traceparent'] ?? null);
        self::assertMatchesRegularExpression(
            sprintf('~^00-%s-%s-0[13]$~', $exporter['traceId'], $exporter['spanId']),
            $headers['traceparent'],
        );
    }

    public function testStepProcessContinuesExportedTrace(): void
    {
        $exporter = $this->runPhp(self::EXPORT_SCRIPT);
        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertSame('cli.command', $importer['name']);
        self::assertSame($exporter['traceId'], $importer['traceId']);
        self::assertSame($exporter['spanId'], $importer['parentId']);
    }

    public function testImportWithoutFileStartsOwnTrace(): void
    {
        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertSame('cli.command', $importer['name']);
        self::assertNull($importer['parentId']);
    }

    public function testImportOfDatadogContextStartsOwnTrace(): void
    {
        // written by a service container still built with the Datadog tracer
        file_put_contents(
            $this->dataDirPath . '/' . StepTraceContext::FILE_NAME,
            (string) json_encode([
                'x-datadog-trace-id' => '1234567890123456789',
                'x-datadog-parent-id' => '987654321',
                'x-datadog-sampling-priority' => '1',
            ]),
        );

        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertNull($importer['parentId']);
    }

    public function testImportIgnoresMalformedFile(): void
    {
        file_put_contents($this->dataDirPath . '/' . StepTraceContext::FILE_NAME, '{"traceparent": ');

        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertNull($importer['parentId']);
    }

    /**
     * @return array<string, mixed>
     */
    private function runPhp(string $code): array
    {
        $script = sprintf(
            "<?php\nrequire %s;\n%s\n",
            var_export(__DIR__ . '/../../vendor/autoload.php', true),
            $code,
        );
        $scriptPath = sys_get_temp_dir() . '/' . uniqid('step-trace-context-') . '.php';
        file_put_contents($scriptPath, $script);

        try {
            $process = new Process(
                ['php', '-d', 'zend.assertions=1', $scriptPath, $this->dataDirPath],
                env: [
                    'OTEL_PHP_AUTOLOAD_ENABLED' => 'true',
                    'OTEL_TRACES_EXPORTER' => 'none',
                    'OTEL_METRICS_EXPORTER' => 'none',
                    'OTEL_LOGS_EXPORTER' => 'none',
                    'OTEL_PROPAGATORS' => 'tracecontext,baggage',
                ],
            );
            $process->mustRun();
        } finally {
            unlink($scriptPath);
        }

        $output = json_decode($process->getOutput(), true, flags: JSON_THROW_ON_ERROR);
        self::assertIsArray($output);
        /** @var array<string, mixed> $output */
        return $output;
    }
}
