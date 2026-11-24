<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Tracing;

use Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process;

class StepTraceContextTest extends TestCase
{
    private const EXPORT_SCRIPT = <<<'PHP'
        $span = OpenTelemetry\API\Globals::tracerProvider()->getTracer('test')->spanBuilder('job.step')->startSpan();
        $scope = $span->activate();
        (new Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext($argv[1]))->export();
        $spanContext = $span->getContext();
        echo json_encode(['traceId' => $spanContext->getTraceId(), 'spanId' => $spanContext->getSpanId()]);
        $scope->detach();
        $span->end();
        PHP;

    private const IMPORT_SCRIPT = <<<'PHP'
        (new Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext($argv[1]))->import('input-mapping');
        $root = OpenTelemetry\API\Trace\LocalRootSpan::current();
        assert($root instanceof OpenTelemetry\SDK\Trace\ReadableSpanInterface);
        $parent = $root->getParentContext();
        echo json_encode([
            'name' => $root->getName(),
            'traceId' => $root->getContext()->getTraceId(),
            'parentId' => $parent->isValid() ? $parent->getSpanId() : null,
        ]);
        PHP;

    // bin/run of the service container: the process span of the job, the step span that exports the context
    private const SERVICE_CONTAINER_SCRIPT = <<<'PHP'
        Keboola\JobQueue\Telemetry\Tracing::startProcessSpan('app:run');
        $span = Keboola\JobQueue\Telemetry\Tracing::tracer()->spanBuilder('job.step')->startSpan();
        $scope = $span->activate();
        (new Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext($argv[1]))->export();
        $stepSpan = $span->getContext();
        echo json_encode(['traceId' => $stepSpan->getTraceId(), 'spanId' => $stepSpan->getSpanId()]);
        $scope->detach();
        $span->end();
        PHP;

    // the context of the daemon span that starts the job pod, the daemon sets it on every container of the pod
    private const JOB_POD_ENV = [
        'TRACEPARENT' => '00-' . self::DAEMON_TRACE_ID . '-' . self::DAEMON_SPAN_ID . '-03',
    ];
    private const DAEMON_TRACE_ID = 'ff13895834ec59884b055321feabc0d2';
    private const DAEMON_SPAN_ID = '477831bf7b85dc8e';

    private string $dataDirPath;

    protected function setUp(): void
    {
        parent::setUp();

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
        self::assertSame(['traceparent'], array_keys($headers));
    }

    public function testStepProcessContinuesExportedTrace(): void
    {
        $exporter = $this->runPhp(self::EXPORT_SCRIPT);
        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertSame('input-mapping', $importer['name']);
        self::assertSame($exporter['traceId'], $importer['traceId']);
        self::assertSame($exporter['spanId'], $importer['parentId']);
    }

    public function testImportWithoutFileStartsOwnTrace(): void
    {
        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertSame('input-mapping', $importer['name']);
        self::assertNull($importer['parentId']);
    }


    public function testImportIgnoresMalformedFile(): void
    {
        file_put_contents($this->dataDirPath . '/' . StepTraceContext::FILE_NAME, '{"traceparent": ');

        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertNull($importer['parentId']);
    }

    public function testJobPodProcessesJoinTraceOfDaemon(): void
    {
        $serviceContainer = $this->runPhp(self::SERVICE_CONTAINER_SCRIPT, self::JOB_POD_ENV);
        $step = $this->runPhp(self::IMPORT_SCRIPT, self::JOB_POD_ENV);

        // the service container continues the daemon span, the step continues the step span from the file
        self::assertSame(self::DAEMON_TRACE_ID, $serviceContainer['traceId']);
        self::assertSame(self::DAEMON_TRACE_ID, $step['traceId']);
        self::assertSame($serviceContainer['spanId'], $step['parentId']);
    }

    public function testImportWithoutFileContinuesTraceOfJobPod(): void
    {
        $importer = $this->runPhp(self::IMPORT_SCRIPT, self::JOB_POD_ENV);

        self::assertSame(self::DAEMON_TRACE_ID, $importer['traceId']);
        self::assertSame(self::DAEMON_SPAN_ID, $importer['parentId']);
    }


    public function testImportOfMalformedFileContinuesTraceOfJobPod(): void
    {
        file_put_contents($this->dataDirPath . '/' . StepTraceContext::FILE_NAME, '{"traceparent": ');

        $importer = $this->runPhp(self::IMPORT_SCRIPT, self::JOB_POD_ENV);

        self::assertSame(self::DAEMON_TRACE_ID, $importer['traceId']);
        self::assertSame(self::DAEMON_SPAN_ID, $importer['parentId']);
    }

    /**
     * @param array<string, string> $env
     * @return array<string, mixed>
     */
    private function runPhp(string $code, array $env = []): array
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
                    // not inherited from the environment of the test run, false removes a variable
                    'TRACEPARENT' => false,
                    'TRACESTATE' => false,
                    ...$env,
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
