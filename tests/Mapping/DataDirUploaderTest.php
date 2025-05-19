<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\Tests\Mapping;

use Keboola\JobQueue\JobConfiguration\Mapping\DataDirUploader;
use Keboola\JobQueue\JobConfiguration\Mapping\SecretsRedactorInterface;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;
use ZipArchive;

class DataDirUploaderTest extends TestCase
{
    public function testUploadDataDir(): void
    {
        $dataDirPath = sys_get_temp_dir() . '/data-dir-uploader-test';
        $zipUploadPath = sys_get_temp_dir() . '/data-dir-uploader-test-upload.zip';

        $this->prepareDataDir(
            $dataDirPath,
            [
                'config.json' => '{"foo": "config.json"}',
                'in/tables/table.csv' => "foo,bar\n1,2",
                'out/tables/table.csv' => "foo,baz\n1,3",
            ],
        );

        $storageApiClient = $this->createMock(BranchAwareClient::class);
        $storageApiClient->expects(self::once())
            ->method('uploadFile')
            ->with(
                self::isType('string'),
                (new FileUploadOptions())
                    ->setTags([
                        'debug',
                        'test-component',
                        'jobId:job-123',
                        'rowId:test-row-id',
                    ])
                    ->setIsPermanent(false)
                    ->setIsPublic(false)
                    ->setNotify(false),
            )
            ->willReturnCallback(function (string $zipPathname) use ($zipUploadPath) {
                // copy the zip elsewhere to simulate upload
                // do JUST a copy, not move - uploader is responsible for the ZIP file cleanup
                (new Filesystem())->copy($zipPathname, $zipUploadPath);
            })
        ;

        $secretsRedactor = $this->createMock(SecretsRedactorInterface::class);
        $secretsRedactor->method('redactSecrets')->willReturnArgument(0);

        $uploader = new DataDirUploader(
            $storageApiClient,
            $secretsRedactor,
        );

        $uploader->uploadDataDir(
            'job-123',
            'test-component',
            'test-row-id',
            $dataDirPath,
            'state_test-step',
        );

        self::assertZipFileContentsEquals(
            $dataDirPath,
            $zipUploadPath,
        );
    }

    public function testSecretsAreRedactedInSelectedFiles(): void
    {
        // prepare standard data dir
        $dataDirPath = sys_get_temp_dir() . '/data-dir-uploader-test-regular';
        $this->prepareDataDir(
            $dataDirPath,
            [
                'config.json' => '{"foo": "config.json"}',
                'in/state.json' => '{"foo": "in/state.json"}',
                'in/tables/table.csv' => "foo,bar\n1,2",
                'out/state.json' => '{"foo": "out/state.json"}',
                'out/tables/table.csv' => "foo,baz\n1,3",
            ],
        );

        // prepare a data dir with masked files to compare to
        $maskedDataDirPath = sys_get_temp_dir() . '/data-dir-uploader-test-masked';
        $this->prepareDataDir(
            $maskedDataDirPath,
            [
                'config.json' => '{"foo": "***1"}',
                'in/state.json' => '{"foo": "***2"}',
                'in/tables/table.csv' => "foo,bar\n1,2",
                'out/state.json' => '{"foo": "***3"}',
                'out/tables/table.csv' => "foo,baz\n1,3",
            ],
        );

        $zipUploadPath = sys_get_temp_dir() . '/data-dir-uploader-test-masked-upload.zip';

        $storageApiClient = $this->createMock(BranchAwareClient::class);
        $storageApiClient->expects(self::once())
            ->method('uploadFile')
            ->with(
                self::isType('string'),
                self::isInstanceOf(FileUploadOptions::class),
            )
            ->willReturnCallback(function (string $zipPathname) use ($zipUploadPath) {
                (new Filesystem())->copy($zipPathname, $zipUploadPath);
            })
        ;

        $secretRedactor = $this->createMock(SecretsRedactorInterface::class);
        $secretRedactor->expects(self::exactly(3))
            ->method('redactSecrets')
            ->with(self::isType('string'))
            ->willReturnCallback(function (string $fileContent) {
                return match ($fileContent) {
                    '{"foo": "config.json"}' => '{"foo": "***1"}',
                    '{"foo": "in/state.json"}' => '{"foo": "***2"}',
                    '{"foo": "out/state.json"}' => '{"foo": "***3"}',
                    default => throw new AssertionFailedError('Unexpected invocation of maskSecrets()'),
                };
            })
        ;

        $uploader = new DataDirUploader(
            $storageApiClient,
            $secretRedactor,
        );

        $uploader->uploadDataDir(
            'job-123',
            'test-component',
            'test-row-id',
            $dataDirPath,
            'state_test-step',
        );

        self::assertZipFileContentsEquals(
            $maskedDataDirPath,
            $zipUploadPath,
        );
    }

    /**
     * @param array<string, string> $files
     */
    private function prepareDataDir(string $dataDirPath, array $files): void
    {
        $fs = new Filesystem();
        foreach ([
            'in/files',
            'in/tables',
            'in/user',
            'out/files',
            'out/tables',
            'out/user',
        ] as $dirPath) {
            $fs->mkdir($dataDirPath . '/' . $dirPath);
        }

        foreach ($files as $filename => $content) {
            $fs->dumpFile($dataDirPath . '/' . $filename, $content);
        }
    }

    private static function assertZipFileContentsEquals(string $dataDirPath, string $zipPathname): void
    {
        $tempDir = new Temp();

        try {
            $zip = new ZipArchive();
            $zip->open($zipPathname);
            $zip->extractTo($tempDir->getTmpFolder());

            self::assertDirectoryContentsSame($dataDirPath, $tempDir->getTmpFolder());
        } finally {
            $tempDir->remove();
        }
    }

    private static function assertDirectoryContentsSame(string $expected, string $actual): void
    {
        $fs = new Filesystem();
        if (!$fs->exists($expected)) {
            throw new AssertionFailedError(sprintf(
                'Expected path "%s" does not exist',
                $expected,
            ));
        }
        if (!$fs->exists($actual)) {
            throw new AssertionFailedError(sprintf(
                'Actual path "%s" does not exist',
                $actual,
            ));
        }
        $expected = realpath($expected);
        $actual = realpath($actual);
        $diffCommand = [
            'diff',
            '--exclude=.gitkeep',
            '--ignore-all-space',
            '--recursive',
            $expected,
            $actual,
        ];
        $diffProcess = new Process($diffCommand);
        $diffProcess->run();
        if ($diffProcess->getExitCode() > 0) {
            throw new AssertionFailedError(sprintf(
                'Two directories are not the same:' . PHP_EOL .
                '%s' . PHP_EOL .
                '%s' . PHP_EOL .
                '%s' . PHP_EOL .
                '%s',
                $expected,
                $actual,
                $diffProcess->getOutput(),
                $diffProcess->getErrorOutput(),
            ));
        }
    }
}
