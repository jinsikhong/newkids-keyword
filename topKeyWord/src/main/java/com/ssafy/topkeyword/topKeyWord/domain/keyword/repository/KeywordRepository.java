package com.ssafy.topkeyword.topKeyWord.domain.keyword.repository;

import com.ssafy.topkeyword.topKeyWord.domain.keyword.Keyword;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KeywordRepository extends JpaRepository<Keyword, Long> {
}
