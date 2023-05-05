<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class StateDefinition implements ConfigurationInterface
{
    public const NAMESPACE_COMPONENT = 'component';
    public const NAMESPACE_STORAGE = 'storage';
    public const NAMESPACE_INPUT = 'input';
    public const NAMESPACE_TABLES = 'tables';
    public const NAMESPACE_FILES = 'files';

    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('state');
        $root = $treeBuilder->getRootNode();
        $root
            ->children()
                ->arrayNode(self::NAMESPACE_COMPONENT)->prototype('variable')->end()->end()
                ->arrayNode(self::NAMESPACE_STORAGE)
                    ->children()
                        ->arrayNode(self::NAMESPACE_INPUT)
                            ->children()
                                ->arrayNode(self::NAMESPACE_TABLES)
                                    ->prototype('array')
                                        ->children()
                                            ->scalarNode('source')->isRequired()->end()
                                            ->scalarNode('lastImportDate')->isRequired()->end()
                                        ->end()
                                    ->end()
                                ->end()
                                ->arrayNode(self::NAMESPACE_FILES)
                                    ->prototype('array')
                                        ->children()
                                            ->arrayNode('tags')->isRequired()
                                                ->prototype('array')
                                                    ->children()
                                                        ->scalarNode('name')->end()
                                                        ->scalarNode('match')->end()
                                                    ->end()
                                                ->end()
                                            ->end()
                                            ->scalarNode('lastImportId')->isRequired()->end()
                                        ->end()
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }
}
