package com.ssafy.topkeyword.topKeyWord.domain.article.repository;

import com.querydsl.core.types.Projections;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.ssafy.topkeyword.topKeyWord.api.service.article.TopKeywordDto;
import com.ssafy.topkeyword.topKeyWord.api.service.article.TopKeywordResponse;
import com.ssafy.topkeyword.topKeyWord.domain.article.QArticle;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import java.util.List;

import static com.ssafy.topkeyword.topKeyWord.domain.article.QArticle.article;

@Repository
public class ArticleQueryRepository {

    private final JPAQueryFactory queryFactory;

    public ArticleQueryRepository(EntityManager em){
        this.queryFactory = new JPAQueryFactory(em);
    }

    public List<TopKeywordDto> getTopKeyword() {
        return queryFactory
                .select(Projections.constructor(TopKeywordDto.class,
                        article.id,
                        article.topKeywords))
                .from(article)
                .fetch();
    }
}
