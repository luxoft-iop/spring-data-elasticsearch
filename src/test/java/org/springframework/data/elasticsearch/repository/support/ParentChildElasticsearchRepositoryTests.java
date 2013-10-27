/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.elasticsearch.repository.support;

import org.apache.commons.collections.IteratorUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.ChildEntity;
import org.springframework.data.elasticsearch.ParentEntity;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.repositories.ChildElasticsearchRepository;
import org.springframework.data.elasticsearch.repositories.ParentElasticsearchRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

import static org.apache.commons.lang.RandomStringUtils.randomNumeric;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * @author Maksim Sidorov
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:/simple-repository-test.xml")
public class ParentChildElasticsearchRepositoryTests {

    @Resource
    private ChildElasticsearchRepository childRepository;

    @Resource
    private ParentElasticsearchRepository parentRepository;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;


    @Before
    public void before() {
        elasticsearchTemplate.deleteIndex(ParentEntity.class);
        elasticsearchTemplate.createIndex(ParentEntity.class);
        elasticsearchTemplate.refresh(ParentEntity.class, true);
        elasticsearchTemplate.putMapping(ParentEntity.class);
        elasticsearchTemplate.putMapping(ChildEntity.class);
    }

	@Test
	public void shouldSearchDocumentsGivenParentSearchQuery() {
		// given
        String parentId1 = randomNumeric(5);
        ParentEntity parentEntity1 = new ParentEntity();
        parentEntity1.setId(parentId1);
        parentEntity1.setText("parent_text_1");

        String parentId2 = randomNumeric(5);
        ParentEntity parentEntity2 = new ParentEntity();
        parentEntity2.setId(parentId2);
        parentEntity2.setText("parent_text_2");

        String childId1 = randomNumeric(5);
        ChildEntity childEntity1 = new ChildEntity();
        childEntity1.setId(childId1);
        childEntity1.setParentId(parentId1);
        childEntity1.setText("child_text_1");

        String childId2 = randomNumeric(5);
        ChildEntity childEntity2 = new ChildEntity();
        childEntity2.setId(childId2);
        childEntity2.setParentId(parentId1);
        childEntity2.setText("child_text_2");

        String childId3 = randomNumeric(5);
        ChildEntity childEntity3 = new ChildEntity();
        childEntity3.setId(childId3);
        childEntity3.setParentId(parentId2);
        childEntity3.setText("child_text_2");

		// when
        parentRepository.save(Arrays.asList(parentEntity1, parentEntity2));
        childRepository.save(Arrays.asList(childEntity1, childEntity2, childEntity3));

		// then

        // simple query
        TermQueryBuilder simpleBuilder = QueryBuilders.termQuery("text", "child_text_2");
        List<ChildEntity> children = IteratorUtils.toList(childRepository.search(simpleBuilder).iterator());

		assertThat(children, hasSize(2));
        assertThat(children.get(0).getId(), equalTo(childId2));
        assertThat(children.get(1).getId(), equalTo(childId3));

        // parent query
        BoolQueryBuilder parentBuilder = QueryBuilders.boolQuery();
        parentBuilder.must(QueryBuilders.termQuery("text", "child_text_2"));
        parentBuilder.must(QueryBuilders.hasParentQuery("parententity", QueryBuilders.termQuery("text", "parent_text_1")));
        // We should use PageRequest search, since single search calls _count request and it doesn't work with hasParent
        // filter in ES 0.90.2, detailed: https://github.com/elasticsearch/elasticsearch/issues/3190
        children = IteratorUtils.toList(childRepository.search(parentBuilder, new PageRequest(0, 10)).iterator());

        assertThat(children, hasSize(1));
        assertThat(children.get(0).getId(), equalTo(childId2));
	}
}
