package com.ssafy.topkeyword.topKeyWord.domain.article.repository;

import com.ssafy.topkeyword.topKeyWord.domain.article.Article;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ArtircleRepository extends JpaRepository<Article, Long> {
}
