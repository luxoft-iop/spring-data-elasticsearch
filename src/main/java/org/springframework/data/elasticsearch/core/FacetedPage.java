package org.springframework.data.elasticsearch.core;

import org.elasticsearch.search.SearchHits;
import org.springframework.data.domain.Page;
import org.springframework.data.elasticsearch.core.facet.FacetResult;

import java.util.List;

/**
 *
 * @author Rizwan Idrees
 * @author Mohsin Husen
 * @author Artur Konczak
 * @author Jonathan Yan
 */
public interface FacetedPage<T> extends Page<T> {

    boolean hasFacets();

    List<FacetResult> getFacets();

    FacetResult getFacet(String name);

    /**
     * Temp solution for receiving original hits
     * @author Maksym Sydorov
     */
    SearchHits getHits();
}
