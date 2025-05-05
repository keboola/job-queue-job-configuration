<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration;

use Keboola\InputMapping\Configuration\File as InputFile;
use Keboola\InputMapping\Configuration\Table as InputTable;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Authorization\AuthorizationDefinition;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Runtime\WorkspaceCredentials\Type;
use Keboola\OutputMapping\Configuration\File as OutputFile;
use Keboola\OutputMapping\Configuration\Table as OutputTable;
use Keboola\OutputMapping\Configuration\TableFile as OutputTableFile;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ConfigurationDefinition implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('configuration');
        $root = $treeBuilder->getRootNode();
        // System
        $root
            ->children()
                ->variableNode('parameters')->end()
                ->arrayNode('runtime')
                    ->ignoreExtraKeys(false)
                    ->children()
                        ->booleanNode('safe')->defaultNull()->end()
                        ->scalarNode('image_tag')->defaultNull()->end()
                        ->arrayNode('backend')
                            ->ignoreExtraKeys(true)
                            ->treatNullLike([])
                            ->addDefaultsIfNotSet()
                            ->children()
                                ->scalarNode('type')->end()
                                ->scalarNode('context')->end()
                                ->arrayNode('workspace_credentials')
                                    ->ignoreExtraKeys(false)
                                    ->beforeNormalization()
                                        ->ifNull()
                                        ->thenUnset()
                                    ->end()
                                    ->children()
                                        ->scalarNode('id')
                                            ->isRequired()
                                            ->cannotBeEmpty()
                                        ->end()
                                        ->enumNode('type')
                                            ->isRequired()
                                            ->values(array_map(fn(Type $v) => $v->value, Type::cases()))
                                        ->end()
                                        ->scalarNode('#password')->end()
                                        ->scalarNode('#privateKey')->end()
                                    ->end()
                                    ->validate()
                                        ->ifTrue(fn(array $v) => count(array_filter([
                                            $v['#password'] ?? null,
                                            $v['#privateKey'] ?? null,
                                        ])) !== 1)
                                        ->thenInvalid('Exactly one of "password" or "privateKey" must be configured.')
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->scalarNode('variables_id')->end()
                ->scalarNode('variables_values_id')->end()
                ->scalarNode('shared_code_id')->end()
                ->arrayNode('shared_code_row_ids')
                    ->prototype('scalar')->end()
                ->end()
                ->variableNode('image_parameters')->end()
        ;
        $storage = $root
            ->children()
                ->arrayNode('storage');

        $input = $storage
            ->children()
                ->arrayNode('input');

        $inputTable = $input
            ->children()
                ->scalarNode('read_only_storage_access')
                    ->validate()
                        ->ifTrue(fn ($v) => $v !== null && !is_bool($v))
                        ->thenInvalid('must be boolean or null')
                    ->end()
                ->end()
                ->arrayNode('tables')
                    ->prototype('array')
        ;
        InputTable::configureNode($inputTable);

        $inputFile = $input
            ->children()
                ->arrayNode('files')
                    ->prototype('array')
        ;
        InputFile::configureNode($inputFile);

        $output = $storage
            ->children()
                ->arrayNode('output');

        $outputTable = $output
            ->children()
                ->scalarNode('default_bucket')->end()
                ->enumNode('data_type_support')
                    ->values(DataTypeSupport::values())
                ->end()
                ->enumNode('table_modifications')
                    ->beforeNormalization()
                        ->ifNull()
                        ->thenUnset()
                    ->end()
                    ->values(TableModifications::values())
                ->end()
                ->variableNode('treat_values_as_null')->end()
                ->arrayNode('tables')
                    ->prototype('array')
        ;
        OutputTable::configureNode($outputTable);

        $outputFile = $output
            ->children()
                ->arrayNode('files')
                    ->prototype('array')
        ;
        OutputFile::configureNode($outputFile);

        $outputTableFile = $output
            ->children()
                ->arrayNode('table_files')
        ;
        OutputTableFile::configureNode($outputTableFile);

        // authorization
        $root->children()->append((new AuthorizationDefinition())->getConfigTreeBuilder()->getRootNode());

        // action
        $root->children()->scalarNode('action')->end();

        // processors
        $root->children()
            ->arrayNode('processors')
                ->children()
                    ->arrayNode('before')
                        ->arrayPrototype()
                            ->children()
                                ->variableNode('parameters')->end()
                                ->arrayNode('definition')
                                    ->isRequired()
                                    ->children()
                                        ->scalarNode('component')
                                            ->isRequired()
                                            ->cannotBeEmpty()
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                    ->arrayNode('after')
                        ->arrayPrototype()
                            ->children()
                                ->variableNode('parameters')->end()
                                ->arrayNode('definition')
                                    ->isRequired()
                                    ->children()
                                        ->scalarNode('component')
                                            ->isRequired()
                                            ->cannotBeEmpty()
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ->end();

        // artifacts
        $root->children()
            ->arrayNode('artifacts')
            ->children()
                ->arrayNode('options')
                    ->children()
                        ->booleanNode('zip')
                            ->defaultTrue()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('runs')
                    ->validate()
                        ->ifTrue(function ($v) {
                            if ($v['enabled'] === true) {
                                if (!isset($v['filter']['date_since']) && !isset($v['filter']['limit'])) {
                                    return true;
                                }
                            }
                            return false;
                        })
                        ->thenInvalid('At least one of "date_since" or "limit" parameters must be defined.')
                    ->end()
                    ->children()
                        ->booleanNode('enabled')->defaultFalse()->end()
                        ->arrayNode('filter')
                            ->children()
                                ->scalarNode('date_since')->end()
                                ->integerNode('limit')->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('custom')
                    ->validate()
                        ->ifTrue(function ($v) {
                            if ($v['enabled'] === true) {
                                //phpcs:ignore Generic.Files.LineLength.MaxExceeded
                                if (!isset($v['filter']['component_id']) && !isset($v['filter']['config_id']) && !isset($v['filter']['branch_id'])) {
                                    return true;
                                }
                            }
                            return false;
                        })
                        ->thenInvalid('"component_id", "config_id" and "branch_id" parameters must be defined.')
                    ->end()
                    ->children()
                        ->booleanNode('enabled')->defaultFalse()->end()
                        ->arrayNode('filter')
                            ->children()
                                ->scalarNode('component_id')->end()
                                ->scalarNode('config_id')->end()
                                ->scalarNode('branch_id')->end()
                                ->scalarNode('date_since')->end()
                                ->integerNode('limit')->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('shared')
                    ->children()
                        ->booleanNode('enabled')->defaultFalse()->end()
                    ->end()
                ->end()
            ->end()
        ->end();
        return $treeBuilder;
    }
}
