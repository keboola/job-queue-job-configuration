<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class StateDefinition implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('state');
        $root = $treeBuilder->getRootNode();
        $root
            ->children()
                ->arrayNode('component')
                    ->ignoreExtraKeys(false)
                    ->normalizeKeys(false)
                    ->addDefaultsIfNotSet()
                ->end()
                ->arrayNode('storage')
                    ->children()
                        ->arrayNode('input')
                            ->children()
                                ->arrayNode('tables')
                                    ->arrayPrototype()
                                        ->children()
                                            ->scalarNode('source')->isRequired()->end()
                                            ->scalarNode('lastImportDate')->isRequired()->end()
                                        ->end()
                                    ->end()
                                ->end()
                                ->arrayNode('files')
                                    ->arrayPrototype()
                                        ->children()
                                            ->arrayNode('tags')
                                                ->isRequired()
                                                ->arrayPrototype()
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
                ->arrayNode('data_app')
                    ->ignoreExtraKeys(false)
                    ->normalizeKeys(false)
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }
}
