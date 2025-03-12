<?php

declare(strict_types=1);

namespace Keboola\JobQueue\JobConfiguration\JobDefinition\State;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
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
                ->arrayNode('component')->prototype('variable')->end()->end()
                ->arrayNode('storage')
                    ->children()
                        ->arrayNode('input')
                            ->children()
                                ->arrayNode('tables')
                                    ->prototype('array')
                                        ->children()
                                            ->scalarNode('source')->isRequired()->end()
                                            ->scalarNode('lastImportDate')->isRequired()->end()
                                        ->end()
                                    ->end()
                                ->end()
                                ->arrayNode('files')
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
