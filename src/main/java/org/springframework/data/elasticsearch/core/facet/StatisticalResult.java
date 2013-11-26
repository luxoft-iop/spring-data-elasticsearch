package org.springframework.data.elasticsearch.core.facet;

import org.elasticsearch.search.facet.statistical.StatisticalFacet;

/**
 * Temp solution for Statistical Facet, need complete implementation
 * @author Maksym Sydorov
 */
public class StatisticalResult extends AbstactFacetResult {
    private final StatisticalFacet facet;

    protected StatisticalResult(String name, StatisticalFacet facet) {
        super(name, FacetType.statistical);
        this.facet = facet;
    }

    public StatisticalFacet getFacet() {
        return facet;
    }
}
