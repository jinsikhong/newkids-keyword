package com.ssafy.topkeyword.topKeyWord.domain.keyword.repository;

import com.ssafy.topkeyword.topKeyWord.domain.keyword.ArticleKeyword;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ArticleKeywordRepository extends JpaRepository<ArticleKeyword, Long> {
}
