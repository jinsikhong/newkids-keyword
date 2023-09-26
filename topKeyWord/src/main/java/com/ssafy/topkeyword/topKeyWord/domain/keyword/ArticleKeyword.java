package com.ssafy.topkeyword.topKeyWord.domain.keyword;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ArticleKeyword {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "article_keyword_id")
    private Long id;

    private Long articleKey;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "keyword_id")
    private Keyword keyword;

    @Builder
    private ArticleKeyword(Long articleKey, Keyword keyword) {
        this.articleKey = articleKey;
        this.keyword = keyword;
    }
}
