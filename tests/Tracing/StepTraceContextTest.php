<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Tracing;

use Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process;

class StepTraceContextTest extends TestCase
{
    private string $dataDirPath;

    protected function setUp(): void
    {
        parent::setUp();
        $this->dataDirPath = (new Temp())->getTmpFolder();
    }

    public function testWriteAndRead(): void
    {
        $headers = [
            'x-datadog-trace-id' => '1234567890123456789',
            'x-datadog-parent-id' => '987654321',
            'traceparent' => '00-6ae4ac1100000000112210f47de98115-000000003ade68b1-01',
        ];

        StepTraceContext::write($this->dataDirPath . '/', $headers);

        self::assertFileExists($this->dataDirPath . '/trace-context.json');
        self::assertSame($headers, StepTraceContext::read($this->dataDirPath));
    }

    public function testWriteWithoutHeadersCreatesNoFile(): void
    {
        StepTraceContext::write($this->dataDirPath, []);

        self::assertFileDoesNotExist($this->dataDirPath . '/trace-context.json');
    }

    public function testReadMissingFile(): void
    {
        self::assertSame([], StepTraceContext::read($this->dataDirPath));
    }

    public function testReadMalformedFile(): void
    {
        file_put_contents($this->dataDirPath . '/trace-context.json', '{"x-datadog-trace-id": ');

        self::assertSame([], StepTraceContext::read($this->dataDirPath));
    }

    public function testReadSkipsNonStringValues(): void
    {
        file_put_contents(
            $this->dataDirPath . '/trace-context.json',
            '{"x-datadog-trace-id": "123", "x-datadog-parent-id": 456, "0": "foo", "tags": {"a": "b"}}',
        );

        self::assertSame(['x-datadog-trace-id' => '123'], StepTraceContext::read($this->dataDirPath));
    }

    public function testStepProcessContinuesExportedTrace(): void
    {
        if (!extension_loaded('ddtrace')) {
            self::markTestSkipped('ddtrace extension is not loaded');
        }

        $exporter = $this->runPhp(<<<'PHP'
            $span = DDTrace\start_span();
            Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext::export($argv[1]);
            echo json_encode(['traceId' => DDTrace\trace_id(), 'spanId' => $span->id]);
            DDTrace\close_span();
            PHP);

        $importer = $this->runPhp(<<<'PHP'
            Keboola\JobQueue\JobConfiguration\Tracing\StepTraceContext::import($argv[1]);
            echo json_encode(['traceId' => DDTrace\trace_id(), 'parentId' => DDTrace\root_span()?->parentId]);
            PHP);

        self::assertSame($exporter['traceId'], $importer['traceId']);
        self::assertSame($exporter['spanId'], $importer['parentId']);
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
        $scriptPath = $this->dataDirPath . '/' . uniqid('script-') . '.php';
        file_put_contents($scriptPath, $script);

        $process = new Process(
            ['php', $scriptPath, $this->dataDirPath],
            env: ['DD_TRACE_CLI_ENABLED' => 'true', 'DD_TRACE_AGENT_URL' => 'http://localhost:1'],
        );
        $process->mustRun();

        $output = json_decode($process->getOutput(), true, flags: JSON_THROW_ON_ERROR);
        self::assertIsArray($output);
        /** @var array<string, mixed> $output */
        return $output;
    }
}
