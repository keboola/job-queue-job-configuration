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
        $span = DDTrace\start_span();
        (new Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext($argv[1]))->export();
        echo json_encode(['traceId' => DDTrace\trace_id(), 'spanId' => $span->id]);
        DDTrace\close_span();
        PHP;

    private const IMPORT_SCRIPT = <<<'PHP'
        (new Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext($argv[1]))->import();
        $root = DDTrace\root_span();
        $parentId = isset($root->parentId) ? $root->parentId : null;
        echo json_encode(['traceId' => DDTrace\trace_id(), 'parentId' => $parentId]);
        PHP;

    private string $dataDirPath;

    protected function setUp(): void
    {
        parent::setUp();

        if (!extension_loaded('ddtrace')) {
            self::markTestSkipped('ddtrace extension is not loaded');
        }

        $this->dataDirPath = (new Temp())->getTmpFolder();
    }

    public function testExportWritesActiveSpanContext(): void
    {
        $exporter = $this->runPhp(self::EXPORT_SCRIPT);

        $headers = json_decode(
            (string) file_get_contents($this->dataDirPath . '/' . StepTraceContext::FILE_NAME),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        self::assertIsArray($headers);
        self::assertSame($exporter['spanId'], $headers['x-datadog-parent-id'] ?? null);
        self::assertArrayHasKey('x-datadog-trace-id', $headers);
    }

    public function testExportWithTracerDisabledWritesNoFile(): void
    {
        $this->runPhp(
            '(new Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext($argv[1]))->export(); echo "{}";',
            ['DD_TRACE_ENABLED' => 'false'],
        );

        self::assertFileDoesNotExist($this->dataDirPath . '/' . StepTraceContext::FILE_NAME);
    }

    public function testStepProcessContinuesExportedTrace(): void
    {
        $exporter = $this->runPhp(self::EXPORT_SCRIPT);
        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertSame($exporter['traceId'], $importer['traceId']);
        self::assertSame($exporter['spanId'], $importer['parentId']);
    }

    public function testImportWithoutFileKeepsOwnTrace(): void
    {
        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertNull($importer['parentId']);
    }

    public function testImportIgnoresMalformedFile(): void
    {
        file_put_contents($this->dataDirPath . '/' . StepTraceContext::FILE_NAME, '{"x-datadog-trace-id": ');

        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertNull($importer['parentId']);
    }

    public function testImportSkipsNonStringValues(): void
    {
        file_put_contents(
            $this->dataDirPath . '/' . StepTraceContext::FILE_NAME,
            (string) json_encode([
                'x-datadog-trace-id' => '1234567890123456789',
                'x-datadog-parent-id' => 987654321,
            ]),
        );

        $importer = $this->runPhp(self::IMPORT_SCRIPT);

        self::assertSame('1234567890123456789', $importer['traceId']);
        // ddtrace would accept the int parent id
        self::assertNull($importer['parentId']);
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
                ['php', $scriptPath, $this->dataDirPath],
                env: [
                    'DD_TRACE_CLI_ENABLED' => 'true',
                    'DD_TRACE_AGENT_URL' => 'http://localhost:1',
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
